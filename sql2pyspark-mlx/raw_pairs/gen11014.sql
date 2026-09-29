with cleaned as (
    -- Keep recent, valid rows
    select *, date_add(hire_date, 54) as hire_date_plus_54d
    from employees
    where hire_date > to_date('2023-06-05')
      and level is not null
),
metrics as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over w_grp as salary_percent_rank,
        avg(salary) over w_running as manager_id_avg_salary,
        row_number() over (partition by level order by hire_date_plus_54d desc, id) as seq_no
    from cleaned
    window w_grp as (partition by manager_id order by salary), w_running as (partition by manager_id)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        job_title,
        salary,
        hire_date,
        level,
        seq_no,
        round(seq_no / sum(seq_no) over (partition by manager_id), 4) as seq_no_share,
        first_value(level) ignore nulls over (partition by manager_id order by hire_date_plus_54d, id rows between unbounded preceding and current row) as first_known_level,
        sum(salary_percent_rank) over w_part as cumulative_salary_percent_rank
    from metrics
    where seq_no = 7
    window w_part as (partition by month(hire_date_plus_54d), dept_id order by salary_percent_rank, id rows between unbounded preceding and current row)
),
metrics_employees as (
    -- Running and moving aggregates
    select
        id,
        job_title,
        salary,
        level,
        seq_no_share,
        first_known_level,
        cumulative_salary_percent_rank,
        sum(cumulative_salary_percent_rank) over (partition by date_trunc('week', hire_date), level order by seq_no, id rows between unbounded preceding and current row) as cumulative_cumulative_salary_percent_rank,
        avg(seq_no) over (partition by job_title, level) as job_title_avg_seq_no,
        ntile(10) over w_part as seq_no_share_bucket,
        last_value(seq_no_share) ignore nulls over w_part_2 as last_known_seq_no_share,
        sum(cumulative_salary_percent_rank) over w_running as hire_date_month_sum_cumulative_salary_percent_rank
    from with_windows
    window w_part as (partition by year(hire_date), job_title order by seq_no_share, id), w_part_2 as (partition by job_title, level order by salary, id rows between unbounded preceding and current row), w_running as (partition by month(hire_date), level)
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        job_title,
        cumulative_salary_percent_rank,
        job_title_avg_seq_no,
        seq_no_share_bucket
    from metrics_employees
    qualify rank() over (partition by job_title, level order by seq_no_share nulls first) = 1
)
-- Ranking and gap features
select
    id,
    job_title,
    cumulative_salary_percent_rank,
    job_title_avg_seq_no,
    lag(seq_no_share_bucket) over w_hist as prev_seq_no_share_bucket,
    lag(cumulative_salary_percent_rank) over w_hist as prev_cumulative_salary_percent_rank,
    lead(cumulative_salary_percent_rank) over w_hist as next_cumulative_salary_percent_rank,
    row_number() over w_hist as row_seq
from latest
window w_hist as (partition by job_title order by seq_no_share_bucket, id);
