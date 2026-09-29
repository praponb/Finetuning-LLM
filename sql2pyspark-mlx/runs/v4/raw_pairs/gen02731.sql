with FilteredEmployees as (
    select *
    from employees
    where not (hire_date > '2024-06-01' or job_title in ('Manager', 'Designer', 'Analyst'))
),
agg as (
    select month(hire_date) as hire_month, sum(case when level = 'L1' then 1 else 0 end) as l1_count, min(hire_date) as first_hire_date
    from FilteredEmployees
    group by month(hire_date)
)
select hire_month, l1_count, first_hire_date
from agg
where l1_count < 37
order by l1_count desc, hire_month
limit 5;
