with scored as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by manager_id, level order by hire_date, id) event_seq,
        min(salary) over (partition by manager_id) manager_id_min_salary
    from employees
)
select
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    case when job_title rlike '^(Manager|Engineer)$' then 'GROUP_B' when job_title rlike '^(Analyst|Director)$' then 'PRIORITY' else 'EXTENDED' end as job_title_zone
from scored;
