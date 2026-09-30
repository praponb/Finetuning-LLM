select
    id,
    manager_id,
    email,
    hire_date
from employees
where lower(email) like '%enr%';
