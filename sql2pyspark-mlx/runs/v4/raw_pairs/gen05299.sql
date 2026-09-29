select
    e.id,
    e.full_name as employee_full_name,
    m.manager_id as manager_manager_id,
    coalesce(m.email, 'N/A') as email_filled
from employees as e
left join employees as m on e.manager_id = m.id
where not m.is_remote and not e.is_remote
