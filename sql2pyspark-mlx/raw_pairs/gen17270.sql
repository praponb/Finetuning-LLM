select
    d.id,
    d.dept_name,
    d.location,
    t.min_hire_date,
    t.salary_range
from departments d
join (select dept_id, min(hire_date) min_hire_date, max(salary) - min(salary) salary_range
    from employees
    group by dept_id) t on d.id = t.dept_id
where t.salary_range > 40
order by d.dept_name, id
limit 20;
