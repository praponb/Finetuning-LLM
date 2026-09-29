select
    e.full_name as employee_full_name,
    e.manager_id as employee_manager_id,
    m.job_title as manager_job_title,
    m.is_remote as manager_is_remote
from employees e
left join employees m on e.manager_id = m.id
order by m.job_title;
