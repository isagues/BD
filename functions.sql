create table YEAR
(
    year   integer not null check (year < 2500),
    isleap boolean default false,
    primary key (year)
);

create table QUARTER
(
    id            serial  not null,
    quarternumber integer not null check (quarternumber between 1 and 4),
    yearfk        integer not null,
    primary key (id),
    unique (quarternumber, yearfk),
    foreign key (yearfk) references year
);

create table MONTH
(
    id        serial  not null,
    monthid   integer not null check (monthid between 1 and 12),
    monthdesc varchar(20) check (monthdesc in ('enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre')),
    quarterfk integer not null,
    primary key (id),
    unique (monthid, quarterfk),
    foreign key (quarterfk) references quarter
);

create table DATEDETAIL
(
    id        serial  not null,
    day       integer not null check (day between 1 and 31),
    dayofweek varchar(20) check (dayofweek in ('lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo')),
    weekend   boolean default false,
    monthfk   integer not null,
    primary key (id),
    unique (day, monthfk),
    foreign key (monthfk) references month
);

create table EVENT
(
    Declaration_Number varchar,
    Declaration_Type   varchar check (Declaration_Type in ('Emergency', 'Disaster')),
    State              varchar,
    Disaster_Type      varchar,
    Declaration_Date   integer not null,
    primary key (Declaration_Number)
);

-- Seccion ier -------------------------------------------------------------------------------------------------------------------

create or replace function is_leap_year(year year.year%type)
 returns boolean as
$$
    begin
        return (year % 4 = 0) and ((year % 100 <> 0) or (year % 400 = 0));
    end;
$$ LANGUAGE plpgsql returns null on null input;

create or replace function newYearHandler() returns trigger as
$$
    begin
        if new.year is null then
            raise exception 'year cant be null';
        end if;
        new.isleap = is_leap_year(new.year);
        return new;
    end
$$
    language plpgsql;

create trigger newYear
    before insert
    on YEAR
    for each row
    execute procedure newYearHandler();

-- Seccion cuarter -------------------------------------------------------------------------------------------------------------------
create or replace function newQuarterHandler() returns trigger
as $$
    begin
        raise notice 'Year (nqh): %', new.yearfk;
        perform * from year y where y.year = new.yearfk;
        if not FOUND then
            insert into year(year) values (new.yearfk);
        end if;
        return new;
    end;
$$ language plpgsql;

create trigger newQuarter
    before insert on quarter for each row execute procedure newQuarterHandler();

create or replace function getQuarterNumber(month MONTH.monthid%type) returns quarter.quarternumber%type
 as $$
begin
    return (month - 1) / 3 + 1;
end;
$$ language plpgsql returns null on null input;

-- Secion month
create or replace function getDescription(
month month.monthid%type
) returns month.monthdesc%type
    as $$
        begin
            case month
                when 1 then return 'enero';
                when 2 then return 'febrero';
                when 3 then return 'marzo';
                when 4 then return 'abril';
                when 5 then return 'mayo';
                when 6 then return 'junio';
                when 7 then return 'julio';
                when 8 then return 'agosto';
                when 9 then return 'septiembre';
                when 10 then return 'octubre';
                when 11 then return 'noviembre';
                when 12 then return 'diciembre';
                else raise 'Invalid month ID';
            end case;
        end
    $$
language plpgsql;

create or replace function insertMonth(
    month month.monthid%type,
    year year.year%type
) returns month.id%type
as $$
    declare
        qNumber quarter.quarternumber%type;
        monthDes month.monthdesc%type;
        qFK quarter.id%type;
        monthID month.id%type;
    begin
        raise notice 'Month: %, Year: %', month, year;
        qNumber = getQuarterNumber(month);
        monthDes = getDescription(month);
        select id into qFK from quarter where quarternumber = qNumber and yearfk = year;
        if not found then
            raise notice 'qNumber: %, Year: %', qNumber, year;
            insert into quarter(quarternumber, yearfk) values (qNumber,year) returning id into qFK;
        end if;
        insert into month(monthid, monthdesc, quarterfk) values (month, monthDes, qFK) returning id into monthID;
        return monthID;

        exception when others then raise exception 'insertMonth: (%)', sqlerrm;
    end
$$ language plpgsql returns null on null input;

-- DAET DETAIL -------------------------------------------------------------------------------------------------------------------------------
select extract(isodow from date '2016-12-12');

create or replace function getDayName(
fecha date
) returns month.monthdesc%type
    as $$
        declare
            dayNumber integer;
        begin
            dayNumber = extract(isodow from fecha);
            case dayNumber
                when 0 then return 'domingo';
                when 1 then return 'lunes';
                when 2 then return 'martes';
                when 3 then return 'miercoles';
                when 4 then return 'jueves';
                when 5 then return 'viernes';
                when 6 then return 'sabado';
                else raise 'Invalid date';
            end case;
        end
    $$
language plpgsql;

create or replace function insertDateDetail(
    fecha date
) returns month.id%type
as $$
    declare
        dayVar datedetail.day%type;
        month month.monthid%type;
        year year.year%type;
        dayName datedetail.dayofweek%type;
        isWeekend datedetail.weekend%type;
        monthID month.id%type;
        dateID datedetail.id%type;
    begin
        dayVar = extract(day from fecha);
        month = extract(month from fecha);
        year = extract(year from fecha);
        dayName = getDayName(fecha);
        isWeekend = dayName in ('sabado', 'domingo');

        select m.id into monthID
        from month m
            inner join quarter q on m.quarterfk = q.id
        where m.monthid = month and q.yearfk = year;

        if not found then
            select insertMonth(month, year) into monthID;
        end if;

        insert into datedetail(day, dayofweek, monthfk, weekend) values (dayVar, dayName, monthID, isWeekend) returning id into dateID;
        return dateID;

        exception when others then raise exception 'insertDateDetail: (%)', sqlerrm;
    end
$$ language plpgsql returns null on null input;
