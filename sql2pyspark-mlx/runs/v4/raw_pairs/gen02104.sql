with valid as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        initcap(trim(level)) as level_title,
        concat(job_title, ' ', full_name) as job_title_full_name,
        coalesce(level, 'DIRECT') as resolved_level
    from employees
    where hire_date >= date '2024-02-01'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        level_title,
        job_title_full_name,
        resolved_level,
        row_number() over (order by hire_date, id) as row_seq,
        row_number() over (partition by manager_id order by hire_date desc, id) as row_seq_2
    from valid
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(row_seq) over w_part as row_seq_avg_2_prior,
        avg(row_seq_2) over w_ordered as row_seq_2_avg_3
    from with_windows
    window w_part as (order by hire_date, id rows between 2 preceding and 1 preceding), w_ordered as (order by row_seq_2, id rows between 3 preceding and current row)
),
with_dims as (
    -- Attach employees attributes
    select
        e.*,
        p.level,
        p.full_name as employee_full_name,
        p.email as employee_email
    from scored e
    left join employees p
        on e.manager_id = p.id
)
select
    id,
    full_name,
    email,
    manager_id,
    hire_date,
    job_title_full_name,
    row_seq,
    row_seq_2,
    row_seq_avg_2_prior
from with_dims
