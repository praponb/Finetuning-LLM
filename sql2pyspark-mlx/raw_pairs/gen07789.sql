select
    e.id,
    e.salary employee_salary,
    e.dept_id employee_dept_id,
    e.job_title employee_job_title,
    m.hire_date manager_hire_date,
    m.level manager_level,
    year(m.hire_date) hire_date_year
from employees as e
left join employees as m on e.manager_id = m.id
where m.level in ('L3', 'L1') and m.full_name like 'He%'
