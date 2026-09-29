select
    emp.id,
    emp.level as employee_level,
    emp.hire_date as employee_hire_date,
    mgr.hire_date as manager_hire_date,
    mgr.full_name as manager_full_name,
    mgr.email as manager_email
from employees emp
left join employees mgr on emp.manager_id = mgr.id;
