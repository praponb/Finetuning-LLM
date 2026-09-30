with joined as (
    select s.*, b.job_title as employee_job_title
    from employees s
    left join employees b
        on s.manager_id = b.id
),
enriched as (
    select
        s.*,
        d.level as employee_level,
        d.salary as employee_salary
    from joined s
    join employees d
        on s.manager_id = d.id
),
kept_keys as (
    select id
    from enriched
    intersect all
    select id
    from employees
    where year(hire_date) >= 2024
),
verified as (
    select s.*
    from enriched s
    join kept_keys k on s.id = k.id
),
cleaned as (
    select *, trim(employee_level) clean_employee_level
    from verified
    where employee_level in ('L3', 'L2')
),
scored as (
    select
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        level,
        employee_salary,
        clean_employee_level,
        round(salary / sum(salary) over (partition by employee_job_title), 4) salary_share,
        cume_dist() over (order by salary asc) salary_cume_dist
    from cleaned
),
scored_employees as (
    select
        *,
        round(employee_salary / sum(employee_salary) over (partition by dept_id, level), 4) employee_salary_share,
        cume_dist() over (order by salary desc) salary_cume_dist_2,
        dense_rank() over (partition by level order by salary_cume_dist asc) salary_cume_dist_dense_rank,
        rank() over (partition by dept_id order by salary_cume_dist desc nulls last) salary_cume_dist_rank
    from scored
    where job_title != 'Analyst'
),
metrics as (
    select *, percent_rank() over (partition by level order by salary_cume_dist_2 desc nulls last) salary_cume_dist_2_percent_rank
    from scored_employees
)
select
    full_name,
    email,
    job_title,
    salary,
    level,
    employee_salary,
    clean_employee_level,
    salary_share,
    salary_cume_dist,
    case when full_name rlike '^(Grace Kim|Emma Brown)$' then 'CORE' when full_name rlike '^(Alice Wong|bob smith)$' then 'PRIORITY' else 'OTHER_ZONE' end as full_name_zone
from metrics
order by full_name_zone desc
