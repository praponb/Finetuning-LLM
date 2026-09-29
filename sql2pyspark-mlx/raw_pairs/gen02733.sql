with with_dims as (
    select
        a.id,
        a.full_name,
        a.manager_id,
        a.salary,
        a.hire_date,
        a.is_remote,
        a.level,
        e.hire_date as employee_hire_date
    from employees a
    left join employees e
        on a.manager_id <=> e.id
    where not e.is_remote
),
enriched as (
    select
        src.id,
        src.manager_id,
        src.is_remote,
        src.level,
        src.employee_hire_date,
        p.full_name as employee_full_name,
        p.job_title
    from with_dims src
    inner join employees p
        on src.manager_id = p.id
),
ranked as (
    select
        id,
        employee_hire_date,
        job_title
    from enriched
    qualify rank() over (partition by level order by employee_hire_date asc) = 1
),
latest as (
    select id, employee_hire_date
    from ranked
    qualify row_number() over (partition by job_title order by employee_hire_date asc nulls first, id asc) = 1
)
select id, employee_hire_date
from latest;
