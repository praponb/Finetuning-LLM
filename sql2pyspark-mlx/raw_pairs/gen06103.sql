select
    dim.level as employee_level,
    a.level as employee_level,
    count(*) as cnt,
    avg(a.salary) as avg_salary
from employees a
join employees dim on a.manager_id = dim.id
group by dim.level, a.level;
