select
    emp.id,
    emp.manager_id as employee_manager_id,
    emp.hire_date as employee_hire_date,
    mgr.manager_id as manager_manager_id,
    mgr.hire_date as manager_hire_date
from employees as emp
left join employees as mgr on emp.manager_id = mgr.id
order by emp.hire_date asc, manager_manager_id desc, id asc
limit 5;
