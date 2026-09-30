with filtered_departments as (
    select *, concat(location, ' - ', dept_name) as location_dept_name
    from departments
    where (budget > 2154000 and budget < 2256000) or budget > 2698000
),
dept_name_summary as (
    select dept_name, max(budget) as max_budget
    from filtered_departments
    group by dept_name
)
select *
from dept_name_summary
where max_budget < 3400000
order by max_budget desc;
