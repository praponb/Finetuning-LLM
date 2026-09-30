select
    emp.id,
    emp.email as employee_email,
    emp.hire_date as employee_hire_date,
    emp.dept_id as employee_dept_id,
    mgr.job_title as manager_job_title,
    mgr.salary as manager_salary
from employees as emp
left join employees as mgr on emp.manager_id = mgr.id
where emp.full_name like '%Emm%' and emp.full_name is not null
