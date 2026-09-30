select
    id,
    full_name,
    dept_id,
    case when is_remote then 'Yes' else 'No' end as is_remote_label
from employees
where job_title is not null
