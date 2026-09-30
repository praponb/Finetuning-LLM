select
    is_remote,
    full_name,
    job_title,
    case when job_title in ('Intern', 'Director') then 'Core' else 'Other' end job_title_group
from employees
where hire_date between '2024-07-01' and '2025-02-01' and hire_date <= '2023-08-01';
