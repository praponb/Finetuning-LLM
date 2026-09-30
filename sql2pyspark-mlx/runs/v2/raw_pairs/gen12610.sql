with employees_by_department as (
    select dept_id, round(stddev(salary), 2) as stddev_salary
    from employees
    where email is null
    group by dept_id
)
select d.dept_name, d.location, ebd.stddev_salary
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id
where ebd.stddev_salary > 55
