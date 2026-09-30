select
    d.id,
    d.dept_name,
    d.budget,
    t.salary_range
from departments d
left outer join (select dept_id, max(salary) - min(salary) salary_range
    from employees
    group by dept_id) t on d.id = t.dept_id
where t.salary_range > 53
order by t.salary_range desc
