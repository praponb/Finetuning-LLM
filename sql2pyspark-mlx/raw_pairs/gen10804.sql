select
    d.id,
    d.location,
    count(e.id) as employee_count,
    avg(e.salary) as avg_salary
from departments d
left join employees e on d.id = e.dept_id
where d.dept_name = 'HR'
group by d.id, d.location
order by employee_count, id, location
limit 5;
