with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws(',', job_title, email) as job_title_email_key,
        coalesce(level, 'OTHER') as resolved_level
    from employees
    where job_title is not null
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        resolved_level,
        dense_rank() over (partition by manager_id order by salary desc) as salary_dense_rank,
        lag(salary, 1, 0) over (partition by dept_id, job_title order by hire_date, id) as prev_salary,
        first_value(level, true) over (partition by job_title order by hire_date, id rows between unbounded preceding and current row) as first_known_level
    from filtered
    where upper(full_name) like '%ENR%'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by dept_id, manager_id order by prev_salary nulls last, id) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(full_name)) as full_name_title
    from latest
    where hire_date <= '2024-07-01'
)
-- Window metrics over several different partitions
select
    *,
    avg(prev_salary) over w_ordered as last4_avg_prev_salary,
    max(prev_salary) over w_ordered as last4_max_prev_salary,
    sum(prev_salary) over w_ordered as last4_sum_prev_salary,
    count(prev_salary) over w_ordered as last4_count_prev_salary,
    ntile(5) over (partition by manager_id order by salary_dense_rank, id) as salary_dense_rank_bucket
from base
window w_ordered as (partition by dept_id, manager_id order by salary_dense_rank, id rows between 3 preceding and current row)
