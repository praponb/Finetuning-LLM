with with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (order by budget desc, id) as event_seq,
        lag(budget, 2) over (partition by dept_name, location order by budget, id) as prev_budget
    from departments
)
select
    id,
    dept_name,
    location,
    budget,
    case when dept_name rlike '^(Marketing|Legal)$' then 'PRIORITY' else 'CORE' end as dept_name_zone
from with_windows
order by location desc nulls first;
