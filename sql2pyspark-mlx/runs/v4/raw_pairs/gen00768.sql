with valid as (
    select *, upper(dept_name) as dept_name_upper
    from departments
    where budget <= 3169000
),
matched_keys as (
    select id, location
    from valid
    except all
    select id, location
    from departments
    where dept_name = 'Sales'
),
verified as (
    select s.*
    from valid s
    inner join matched_keys k on s.id = k.id and s.location = k.location
),
valid_departments as (
    select
        id,
        dept_name,
        dept_name_upper,
        cast(budget as int) as budget_int,
        coalesce(location, 'DIRECT') as location_filled
    from verified
    where budget > 2158000
),
base as (
    select
        id,
        dept_name,
        dept_name_upper,
        budget_int,
        round(budget_int, 0) as budget_int_rounded
    from valid_departments
    where budget_int = 70
)
select
    id,
    dept_name,
    dept_name_upper,
    budget_int,
    case when dept_name rlike '^(Sales|Marketing)$' then 'EXTENDED' else 'OTHER_ZONE' end as dept_name_zone
from base
order by dept_name;
