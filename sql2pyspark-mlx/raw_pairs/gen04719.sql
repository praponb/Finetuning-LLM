select *
from (
    select location, dept_name, round(avg(budget), 2) as avg_budget
    from departments
    where location = 'New York'
    group by location, dept_name
) as x
where avg_budget < 40
order by location desc, dept_name asc
limit 20;
