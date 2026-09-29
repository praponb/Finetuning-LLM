select
    d.id,
    d.budget,
    d.dept_name,
    t.last_hire_date,
    t.max_salary
from departments d
inner join (select dept_id, max(hire_date) as last_hire_date, max(salary) as max_salary
    from employees
    group by dept_id) t on d.id = t.dept_id
where t.max_salary >= 111000
order by budget desc;
