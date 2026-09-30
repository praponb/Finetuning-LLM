with with_windows as (
    -- Ranking and gap features
    select
        id,
        email,
        is_remote,
        lag(salary) over (partition by dept_id order by hire_date, id) as prev_salary,
        lead(salary) over (partition by dept_id order by hire_date, id) as next_salary,
        row_number() over (partition by dept_id order by hire_date, id) as row_seq
    from employees
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, trim(email) as clean_email
    from with_windows
    where row_seq between 6 and 7
),
scored as (
    -- Running and moving aggregates
    select
        id,
        email,
        is_remote,
        prev_salary,
        next_salary,
        last_value(prev_salary) ignore nulls over (order by row_seq, id rows between unbounded preceding and current row) as last_known_prev_salary,
        cume_dist() over (order by next_salary desc nulls last) as next_salary_cume_dist,
        sum(next_salary) over (order by row_seq, id rows between unbounded preceding and current row) as cumulative_next_salary
    from filtered
    where is_remote
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        prev_salary,
        last_known_prev_salary,
        next_salary_cume_dist,
        sum(next_salary) over (order by last_known_prev_salary, id rows between unbounded preceding and current row) as cumulative_next_salary_2,
        ntile(4) over (order by next_salary, id) as next_salary_bucket,
        dense_rank() over (order by cumulative_next_salary desc) as cumulative_next_salary_dense_rank
    from scored
)
-- Multi-level aggregation
select
    case when cumulative_next_salary_2 >= 611538 then 'PREMIUM' else 'STANDARD' end as cumulative_next_salary_2_tier,
    case when prev_salary >= 162000 then 'LARGE' else 'SMALL' end as prev_salary_tier,
    approx_percentile(cumulative_next_salary_dense_rank, array(0.25, 0.5, 0.75)) as cumulative_next_salary_dense_rank_quartiles,
    round(avg(next_salary_cume_dist), 2) as avg_next_salary_cume_dist,
    percentile_approx(cumulative_next_salary_2, 0.5) as cumulative_next_salary_2_p50,
    rank() over (partition by case when cumulative_next_salary_2 >= 611538 then 'PREMIUM' else 'STANDARD' end order by round(avg(next_salary_cume_dist), 2) desc) as avg_next_salary_cume_dist_rank
from enriched
group by cumulative_next_salary_2_tier, prev_salary_tier
having round(avg(next_salary_cume_dist), 2) > 0
order by prev_salary_tier desc nulls last
