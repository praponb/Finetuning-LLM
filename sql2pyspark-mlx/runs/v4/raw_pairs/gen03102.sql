with filtered as (
    select
        *,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        coalesce(job_title, 'N/A') as resolved_job_title
    from employees
    where hire_date > date '2024-02-28'
      and level is not null
)
select level
from filtered
except all
select level
from employees
where level in ('L1', 'L2')
