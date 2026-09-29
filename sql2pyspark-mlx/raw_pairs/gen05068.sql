with valid as (
    -- Keep recent, valid rows
    select
        id,
        email,
        job_title,
        hire_date,
        is_remote,
        round(salary * 0.85, 2) as adjusted_salary,
        coalesce(level, 'UNKNOWN') as level_clean
    from employees
    where hire_date >= to_date('2023-03-22')
      and dept_id in (3, 15, 35)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        job_title,
        level_clean,
        avg(adjusted_salary) over (partition by job_title order by adjusted_salary, id rows between 5 preceding and current row) as adjusted_salary_avg_5,
        ntile(4) over (partition by job_title order by adjusted_salary desc, id) as adjusted_salary_bucket,
        sum(adjusted_salary) over w_running as running_adjusted_salary,
        avg(adjusted_salary) over (partition by job_title order by hire_date, id rows between 2 preceding and 1 preceding) as adjusted_salary_avg_2_prior
    from valid
    window w_running as (partition by job_title order by adjusted_salary, id rows between unbounded preceding and current row)
),
valid_employees as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when adjusted_salary_bucket >= 7 then 'High' when adjusted_salary_bucket >= 2 then 'Medium' else 'Low' end as adjusted_salary_bucket_tier,
        adjusted_salary_bucket * adjusted_salary_avg_5 as adjusted_salary_bucket_times_adjusted_salary_avg_5
    from metrics
    where job_title is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by job_title order by running_adjusted_salary desc nulls last) as latest_rank
        from valid_employees
    ) x
    where latest_rank = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        adjusted_salary_bucket,
        adjusted_salary_avg_2_prior,
        adjusted_salary_bucket_tier,
        adjusted_salary_bucket_times_adjusted_salary_avg_5
    from deduped
    qualify row_number() over (partition by job_title order by adjusted_salary_avg_5 nulls last, id) = 1
)
-- Grouped metrics
select
    case when adjusted_salary_bucket_times_adjusted_salary_avg_5 >= 88 then 'LARGE' else 'SMALL' end as adjusted_salary_bucket_times_adjusted_salary_avg_5_tier,
    case when adjusted_salary_bucket >= 8 then 'LARGE' else 'SMALL' end as adjusted_salary_bucket_tier,
    count(distinct id) as distinct_ids,
    approx_percentile(adjusted_salary_bucket, 0.9) as adjusted_salary_bucket_p90,
    dense_rank() over (partition by case when adjusted_salary_bucket_times_adjusted_salary_avg_5 >= 88 then 'LARGE' else 'SMALL' end order by approx_percentile(adjusted_salary_bucket, 0.9) desc) as adjusted_salary_bucket_p90_dense_rank
from ranked
group by adjusted_salary_bucket_times_adjusted_salary_avg_5_tier, adjusted_salary_bucket_tier
having approx_percentile(adjusted_salary_bucket, 0.9) > 0
order by distinct_ids desc
