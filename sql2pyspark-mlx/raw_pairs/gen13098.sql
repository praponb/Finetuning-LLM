select
    e.job_title,
    e.hire_date,
    d.location,
    d.budget,
    round(e.salary * 1.1, 2) as salary_scaled
from employees e
join departments d on e.dept_id = d.id;
