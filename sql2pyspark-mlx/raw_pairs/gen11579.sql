with base as (
    select *, datediff(current_date(), hire_date) as days_since_hire_date
    from employees
    where hire_date >= date '2023-08-26'
),
with_windows as (
    select
        *,
        cume_dist() over (partition by month(hire_date), manager_id order by days_since_hire_date nulls last) as days_since_hire_date_cume_dist,
        row_number() over (partition by level order by days_since_hire_date desc, id) as event_seq,
        dense_rank() over (partition by level order by days_since_hire_date nulls first) as days_since_hire_date_dense_rank
    from base
    where manager_id in (4, 13, 30)
),
joined as (
    select src.*, d.budget
    from with_windows src
    left join departments d
        on src.dept_id = d.id
),
enriched as (
    select
        id,
        dept_id,
        manager_id,
        hire_date,
        days_since_hire_date,
        days_since_hire_date_dense_rank,
        lag(days_since_hire_date_cume_dist) over (partition by level order by days_since_hire_date_dense_rank, id) as prev_days_since_hire_date_cume_dist,
        lag(event_seq) over (partition by level order by days_since_hire_date_dense_rank, id) as prev_event_seq,
        lead(event_seq) over (partition by level order by days_since_hire_date_dense_rank, id) as next_event_seq,
        row_number() over (partition by level order by days_since_hire_date_dense_rank, id) as seq_no,
        dense_rank() over (order by event_seq) as event_seq_dense_rank
    from joined
),
joined_employees as (
    select
        src.id,
        src.dept_id,
        src.manager_id,
        src.hire_date,
        src.days_since_hire_date_dense_rank,
        src.prev_days_since_hire_date_cume_dist,
        src.prev_event_seq,
        src.seq_no,
        src.event_seq_dense_rank,
        d.email,
        d.level
    from enriched src
    join employees d
        on src.manager_id <=> d.id
)
select
    p.job_title,
    e.level as employee_level,
    count(*) as cnt,
    round(avg(e.prev_days_since_hire_date_cume_dist), 2) as avg_prev_days_since_hire_date_cume_dist,
    grouping_id() as gid
from joined_employees e
join employees p on e.manager_id = p.id
group by grouping sets ((p.job_title, e.level), (p.job_title))
