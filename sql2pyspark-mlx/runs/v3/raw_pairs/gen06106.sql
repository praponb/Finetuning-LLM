select d.location, t.latest_hire_date
from departments d
left join (select dept_id, max(hire_date) as latest_hire_date
    from employees
    group by dept_id) t on d.id = t.dept_id
order by latest_hire_date, location;
