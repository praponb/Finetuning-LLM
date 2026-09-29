with employees_by_department as (
    select dept_id, min(salary) as min_salary
    from employees
    group by dept_id
)
select d.budget, ebd.min_salary
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id
