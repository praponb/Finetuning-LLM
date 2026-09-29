with valid as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        concat(full_name, ', ', email) as full_name_email
    from employees
    where job_title is not null
      and job_title is distinct from 'Manager'
),
matched_keys as (
    -- Set intersection on keys
    select level, id
    from valid
    intersect all
    select level, id
    from employees
    where job_title not in ('Engineer', 'Analyst')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.level = k.level and s.id = k.id
),
joined as (
    -- Attach employees attributes
    select
        e.*,
        d.email,
        d.salary as employee_salary
    from verified e
    left join employees d
        on e.manager_id <=> d.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        full_name,
        dept_id,
        salary,
        level,
        email,
        employee_salary,
        cast(employee_salary as int) as employee_salary_int
    from joined
    where job_title is not null
),
metrics as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (order by salary desc) as salary_percent_rank,
        cume_dist() over w_running as employee_salary_int_cume_dist
    from cleaned
    where email like '%@gmail.com'
    window w_running as (partition by dept_id, level order by employee_salary_int desc nulls last)
)
-- Running and moving aggregates
select
    *,
    avg(salary) over (partition by level) as level_avg_salary,
    cume_dist() over (partition by level order by salary_percent_rank) as salary_percent_rank_cume_dist,
    cume_dist() over (partition by level order by employee_salary_int_cume_dist desc) as employee_salary_int_cume_dist_cume_dist,
    max(employee_salary_int) over (partition by dept_id) as dept_id_max_employee_salary_int,
    rank() over (partition by level, dept_id order by employee_salary_int_cume_dist nulls first) as employee_salary_int_cume_dist_rank
from metrics;
