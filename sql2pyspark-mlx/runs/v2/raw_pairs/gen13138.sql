with employees_by_department as (
    select dept_id, count(distinct salary) as unique_levels, avg(salary) as avg_salary
    from employees
    where job_title = 'Intern'
    group by dept_id
)
select
    d.id,
    d.dept_name,
    ebd.unique_levels,
    ebd.avg_salary
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id
