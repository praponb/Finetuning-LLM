with department_totals as (
    select dept_id, min(salary) as min_salary
    from employees
    group by dept_id
)
select d.id, d.budget, dt.min_salary
from departments d
inner join department_totals dt on d.id = dt.dept_id
