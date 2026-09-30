select
    id,
    hire_date,
    upper(trim(email)) as email_normalized,
    if(is_remote, 'Yes', 'No') as is_remote_label
from employees
where manager_id = 15;
