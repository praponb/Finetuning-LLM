with metrics as (
    -- Running and moving aggregates
    select
        *,
        min(budget) over (partition by dept_name) as dept_name_min_budget,
        first_value(dept_name) ignore nulls over (partition by location order by budget, id rows between unbounded preceding and current row) as first_known_dept_name
    from departments
)
select
    id,
    location,
    budget,
    dept_name_min_budget,
    first_known_dept_name,
    case when location rlike '^(Chiang Mai|Bangkok)$' then 'OTHER_ZONE' when location rlike '^(New York|Singapore)$' then 'GROUP_B' else 'EXTENDED' end as location_zone
from metrics
order by location desc;
