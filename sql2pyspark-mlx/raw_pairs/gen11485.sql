with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        level,
        sum(salary) over (partition by manager_id, job_title order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary,
        avg(salary) over (order by salary, id rows between 6 preceding and current row) as salary_avg_6,
        sum(salary) over (partition by level order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary_2,
        sum(salary) over (partition by date_trunc('week', hire_date), job_title order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary_3
    from employees
),
enriched_employees as (
    -- Running and moving aggregates
    select
        *,
        lead(cumulative_salary_2, 2, 0) over (partition by level order by cumulative_salary_3, id) as next_cumulative_salary_2,
        last_value(job_title, true) over (partition by level order by cumulative_salary_2, id rows between unbounded preceding and current row) as last_known_job_title
    from enriched
),
enriched_employees_2 as (
    -- Ranking and gap features
    select
        id,
        email,
        job_title,
        level,
        cumulative_salary,
        cumulative_salary_2,
        last_known_job_title,
        sum(cumulative_salary) over w_part as last3_sum_cumulative_salary,
        min(cumulative_salary) over w_part as last3_min_cumulative_salary,
        max(cumulative_salary) over w_part as last3_max_cumulative_salary
    from enriched_employees
    window w_part as (partition by manager_id, level order by salary_avg_6, id rows between 2 preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by job_title order by cumulative_salary_2, id) as seq_no,
        dense_rank() over (partition by level order by last3_sum_cumulative_salary nulls first) as last3_sum_cumulative_salary_dense_rank
    from enriched_employees_2
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(cumulative_salary_2) over w_hist as next_cumulative_salary_2,
        lag(last3_sum_cumulative_salary_dense_rank) over w_hist as prev_last3_sum_cumulative_salary_dense_rank,
        row_number() over w_hist as row_seq,
        sum(cumulative_salary) over w_hist_2 as cumulative_cumulative_salary
    from with_windows
    window w_hist as (partition by job_title, level order by cumulative_salary_2, id), w_hist_2 as (partition by level order by last3_max_cumulative_salary, id rows between unbounded preceding and current row)
)
-- Grouped metrics
select
    job_title,
    level,
    count(distinct id) as distinct_ids,
    percentile_approx(row_seq, 0.9) as row_seq_p90,
    count_if(level = 'L4') as l4_events,
    min(last3_sum_cumulative_salary) as min_last3_sum_cumulative_salary,
    count(distinct id) as distinct_ids_2,
    dense_rank() over (partition by job_title order by count(distinct id) desc) as distinct_ids_dense_rank
from scored
group by 1, 2;
