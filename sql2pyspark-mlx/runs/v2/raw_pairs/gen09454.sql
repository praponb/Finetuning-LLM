select
    employees.hire_date,
    employees.dept_id,
    employees.email,
    departments.budget,
    departments.location,
    upper(departments.dept_name) as dept_name_upper
from employees
right outer join departments on departments.id = employees.dept_id
where employees.job_title = 'Director'
order by budget desc, dept_name_upper asc;
