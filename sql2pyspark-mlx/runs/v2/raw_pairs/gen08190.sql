select emp.job_title as employee_job_title, emp.salary as employee_salary, mgr.is_remote as manager_is_remote
from employees emp
left join employees mgr on emp.manager_id = mgr.id;
