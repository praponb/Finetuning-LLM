with base as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), hire_date) as days_since_hire_date,
        coalesce(level, 'UNKNOWN') as level_filled
    from employees
    where manager_id is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        email,
        hire_date,
        min(salary) over (partition by dept_id, job_title order by days_since_hire_date, id rows between unbounded preceding and current row) as running_min_salary,
        count(salary) over (partition by dept_id, job_title order by days_since_hire_date, id rows between unbounded preceding and current row) as running_count_salary,
        sum(salary) over (partition by dept_id, job_title order by days_since_hire_date, id rows between unbounded preceding and current row) as running_sum_salary,
        cume_dist() over (partition by month(hire_date) order by salary) as salary_cume_dist
    from base
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        hire_date,
        running_sum_salary,
        lead(running_count_salary) over (order by running_min_salary, id) as next_running_count_salary,
        lag(running_count_salary) over (order by running_min_salary, id) as prev_running_count_salary,
        lead(running_sum_salary) over (order by running_min_salary, id) as next_running_sum_salary,
        lag(running_sum_salary) over (order by running_min_salary, id) as prev_running_sum_salary,
        row_number() over (order by running_min_salary, id) as seq_no,
        row_number() over (order by running_sum_salary desc, id) as seq_no_2
    from with_windows
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        next_running_count_salary,
        prev_running_sum_salary,
        seq_no,
        seq_no_2,
        prev_running_sum_salary * seq_no_2 as prev_running_sum_salary_times_seq_no_2
    from enriched
    where hire_date >= to_date('2023-08-09')
      and prev_running_sum_salary between 415000 and 688000
),
scored as (
    -- Running and moving aggregates
    select
        id,
        next_running_count_salary,
        prev_running_sum_salary,
        row_number() over (order by seq_no, id) as row_seq,
        row_number() over (order by seq_no_2 desc, id) as seq_no_3,
        ntile(4) over (order by next_running_count_salary, id) as next_running_count_salary_bucket,
        sum(seq_no) over (order by next_running_count_salary, id rows between unbounded preceding and current row) as running_seq_no
    from filtered
),
scored_employees as (
    -- Running and moving aggregates
    select
        *,
        sum(row_seq) over w_recent as running_row_seq,
        dense_rank() over (order by prev_running_sum_salary desc nulls last) as prev_running_sum_salary_dense_rank
    from scored
    where seq_no_3 between 4 and 7
    window w_recent as (order by seq_no_3, id rows between unbounded preceding and current row)
),
with_windows_employees as (
    -- Window metrics over several different partitions
    select
        id,
        next_running_count_salary,
        seq_no_3,
        next_running_count_salary_bucket,
        prev_running_sum_salary_dense_rank,
        ntile(10) over (order by seq_no_3 desc, id) as seq_no_3_bucket,
        sum(prev_running_sum_salary) over (order by prev_running_sum_salary_dense_rank, id rows between unbounded preceding and current row) as running_prev_running_sum_salary,
        lag(running_seq_no, 2) over (order by next_running_count_salary_bucket, id) as prev_running_seq_no,
        ntile(5) over (order by prev_running_sum_salary, id) as prev_running_sum_salary_bucket
    from scored_employees
)
-- Grouped metrics
select
    prev_running_seq_no,
    percentile_approx(prev_running_sum_salary_bucket, 0.5) as prev_running_sum_salary_bucket_p50,
    percentile_approx(next_running_count_salary_bucket, 0.95) as next_running_count_salary_bucket_p95,
    round(avg(prev_running_sum_salary_bucket), 2) as avg_prev_running_sum_salary_bucket
from with_windows_employees
group by prev_running_seq_no
order by prev_running_seq_no
