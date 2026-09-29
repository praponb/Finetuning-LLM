with cleaned as (
    select *, upper(trim(email)) as email_normalized
    from employees
    where hire_date > date '2023-02-16'
),
with_windows as (
    select
        id,
        full_name,
        email,
        salary,
        hire_date,
        email_normalized,
        row_number() over (partition by job_title order by salary, id) as seq_no,
        ntile(10) over (partition by dept_id, manager_id order by salary desc, id) as salary_bucket
    from cleaned
    where manager_id = 6
),
cleaned_employees as (
    select
        id,
        full_name,
        email,
        hire_date,
        email_normalized,
        seq_no,
        salary_bucket,
        regexp_replace(email, ' ', '') as email_slug
    from with_windows
    where hire_date > to_date('2023-05-30')
),
valid as (
    select
        id,
        email,
        hire_date,
        email_normalized,
        email_slug,
        round(salary_bucket, 2) as salary_bucket_rounded,
        round(seq_no, 0) as seq_no_rounded
    from cleaned_employees
    where salary_bucket between 4 and 7
),
enriched as (
    select
        *,
        count(seq_no_rounded) over w_recent as last7_count_seq_no_rounded,
        avg(seq_no_rounded) over w_recent as last7_avg_seq_no_rounded,
        min(seq_no_rounded) over w_recent as last7_min_seq_no_rounded,
        sum(salary_bucket_rounded) over w_ordered as cumulative_salary_bucket_rounded
    from valid
    window w_recent as (order by seq_no_rounded, id rows between 6 preceding and current row), w_ordered as (order by hire_date, id rows between unbounded preceding and current row)
),
cleaned_employees_2 as (
    select
        id,
        hire_date,
        email_normalized,
        seq_no_rounded,
        last7_avg_seq_no_rounded,
        last7_min_seq_no_rounded,
        datediff(current_date(), hire_date) / 7 as weeks_since_hire_date
    from enriched
    where cumulative_salary_bucket_rounded is null
),
totals as (
    select
        case when last7_avg_seq_no_rounded >= 84 then 'LARGE' else 'SMALL' end as last7_avg_seq_no_rounded_tier,
        percentile_approx(weeks_since_hire_date, 0.95) as weeks_since_hire_date_p95,
        percentile_approx(last7_min_seq_no_rounded, 0.5) as last7_min_seq_no_rounded_p50,
        approx_percentile(last7_avg_seq_no_rounded, 0.9) as last7_avg_seq_no_rounded_p90,
        percentile_approx(last7_avg_seq_no_rounded, array(0.25, 0.5, 0.75)) as last7_avg_seq_no_rounded_quartiles,
        count(distinct id) as distinct_ids,
        count(distinct id) as distinct_ids_2
    from cleaned_employees_2
    group by 1
    having count(distinct id) > 1
)
select
    last7_avg_seq_no_rounded_tier,
    weeks_since_hire_date_p95,
    last7_min_seq_no_rounded_p50,
    last7_avg_seq_no_rounded_p90,
    last7_avg_seq_no_rounded_quartiles,
    distinct_ids,
    distinct_ids_2
from totals
order by distinct_ids_2 desc
