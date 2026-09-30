select
    id,
    full_name,
    hire_date,
    case when is_remote then 'Yes' else 'No' end as is_remote_label
from employees
where email like '%@outlook.com' and (dept_id in (12, 30) or salary >= 64000)
order by id desc;
