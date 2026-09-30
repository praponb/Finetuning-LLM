with valid as (
    select
        *,
        length(full_name) as full_name_length,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        coalesce(level, 'N/A') as level_clean
    from employees
    where hire_date >= date '2023-06-15'
),
metrics as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        full_name_length,
        avg(salary) over (partition by dept_id order by full_name_length, id rows between 2 preceding and 1 preceding) as salary_avg_2_prior,
        max(full_name_length) over (partition by job_title) as job_title_max_full_name_length
    from valid
    where full_name like '%Won%'
),
with_dims as (
    select
        e.*,
        b.budget,
        b.dept_name
    from metrics e
    left join departments b
        on e.dept_id = b.id
),
scored as (
    select
        *,
        last_value(salary_avg_2_prior) ignore nulls over w_grp as last_known_salary_avg_2_prior,
        min(job_title_max_full_name_length) over w_recent as manager_id_min_job_title_max_full_name_length
    from with_dims
    window w_grp as (partition by date_trunc('week', hire_date), job_title order by full_name_length, id rows between unbounded preceding and current row), w_recent as (partition by manager_id)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by level order by salary_avg_2_prior, id) as row_num
        from scored
    ) x
    where row_num = 1
)
select
    p.level as employee_level,
    src.level as employee_level,
    count(*) as rows_total,
    min(src.budget) as min_budget,
    sum(src.manager_id_min_job_title_max_full_name_length) as total_manager_id_min_job_title_max_full_name_length,
    grouping_id() as gid
from ranked src
inner join employees p on src.manager_id = p.id
group by grouping sets ((p.level, src.level), (p.level));
