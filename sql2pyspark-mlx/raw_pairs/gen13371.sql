with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        coalesce(level, 'Unknown') as level_filled,
        length(full_name) as full_name_length,
        coalesce(level, 'DIRECT') as resolved_level
    from employees
    where hire_date >= date '2023-05-15'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        salary,
        is_remote,
        level_filled,
        full_name_length,
        resolved_level,
        row_number() over (partition by level order by salary, id) as event_seq,
        sum(full_name_length) over w_grp as running_full_name_length,
        round(salary / sum(salary) over (partition by dept_id, level), 4) as salary_share,
        percent_rank() over (partition by dept_id order by salary desc) as salary_percent_rank
    from cleaned
    window w_grp as (partition by level, dept_id order by salary, id rows between unbounded preceding and current row)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        salary,
        level_filled,
        full_name_length,
        event_seq,
        salary_share,
        salary_percent_rank,
        ceil(running_full_name_length) as running_full_name_length_whole
    from scored
    where running_full_name_length != 149
),
filtered_employees as (
    -- Basic cleansing and defaults
    select
        id,
        salary,
        level_filled,
        salary_share,
        salary_percent_rank,
        running_full_name_length_whole,
        running_full_name_length_whole - salary_share as running_full_name_length_whole_minus_salary_share
    from filtered
    where salary_percent_rank >= 0.41
)
-- Multi-level aggregation
select
    case when salary >= 134000 then 'PREMIUM' else 'STANDARD' end as salary_tier,
    round(avg(salary), 2) as avg_salary,
    count(*) as event_count,
    percentile_approx(salary_share, 0.5) as salary_share_p50,
    avg(salary_share) as avg_salary_share
from filtered_employees
group by case when salary >= 134000 then 'PREMIUM' else 'STANDARD' end;
