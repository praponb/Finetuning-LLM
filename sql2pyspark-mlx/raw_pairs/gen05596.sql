with cleaned as (
    select
        *,
        case when level = 'L3' then 1 else 0 end as is_l3,
        coalesce(job_title, 'OTHER') as resolved_job_title
    from employees
    where hire_date >= to_date('2023-07-27')
      and dept_id in (2, 5, 30, 33)
),
latest as (
    select *
    from cleaned
    qualify row_number() over (partition by level order by is_l3 desc nulls last, id) = 1
),
ranked as (
    select
        id,
        salary,
        is_remote,
        is_l3,
        resolved_job_title
    from (
        select *, row_number() over (partition by dept_id order by salary, id) as latest_rank
        from latest
    ) ranked
    where latest_rank = 1
),
metrics as (
    select
        id,
        salary,
        resolved_job_title,
        rank() over (order by salary) as salary_rank,
        lag(salary, 2) over (order by is_l3, id) as prev_salary
    from ranked
),
enriched as (
    select
        id,
        resolved_job_title,
        lead(prev_salary, 2) over (order by salary, id) as next_prev_salary,
        sum(salary_rank) over (order by salary, id rows between unbounded preceding and current row) as running_salary_rank,
        row_number() over (order by salary, id) as event_seq,
        row_number() over (order by salary_rank desc, id) as seq_no
    from metrics
)
select
    case when event_seq >= 7 then 'HIGH' else 'LOW' end as event_seq_tier,
    count(distinct id) as distinct_ids,
    round(avg(event_seq), 2) as avg_event_seq,
    avg(running_salary_rank) as avg_running_salary_rank
from enriched
group by case when event_seq >= 7 then 'HIGH' else 'LOW' end
having round(avg(event_seq), 2) > 0;
