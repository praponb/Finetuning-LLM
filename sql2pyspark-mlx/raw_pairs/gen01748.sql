with base as (
    -- Keep recent, valid rows
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        trim(job_title) as clean_job_title
    from employees
    where level is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        dept_id,
        manager_id,
        job_title,
        is_remote,
        level,
        clean_job_title,
        round(salary, 0) as salary_rounded
    from base
    where level is distinct from 'L2'
),
cleaned_employees as (
    -- Basic cleansing and defaults
    select
        id,
        dept_id,
        manager_id,
        job_title,
        is_remote,
        level,
        clean_job_title,
        salary_rounded,
        cast(salary_rounded as int) as salary_rounded_int,
        case when is_remote then 'Yes' else 'No' end as is_remote_label,
        coalesce(level, 'NONE') as resolved_level
    from cleaned
    where level is distinct from 'L2'
      and manager_id in (18, 25, 28)
),
scored as (
    -- Ranking and gap features
    select
        id,
        dept_id,
        manager_id,
        is_remote,
        clean_job_title,
        salary_rounded,
        resolved_level,
        row_number() over (partition by job_title order by salary_rounded, id) as seq_no,
        avg(salary_rounded) over (partition by level order by salary_rounded, id rows between 6 preceding and 1 preceding) as salary_rounded_avg_6_prior,
        row_number() over (partition by job_title order by salary_rounded_int desc, id) as event_seq,
        max(salary_rounded_int) over (partition by job_title) as job_title_max_salary_rounded_int
    from cleaned_employees
),
enriched as (
    -- Attach employees attributes
    select
        a.*,
        b.email,
        b.level
    from scored a
    left join employees b
        on a.manager_id = b.id
),
enriched_employees as (
    -- Window metrics over several different partitions
    select
        id,
        dept_id,
        manager_id,
        resolved_level,
        email,
        lead(job_title_max_salary_rounded_int) over w_part as next_job_title_max_salary_rounded_int,
        lag(salary_rounded) over w_part as prev_salary_rounded,
        row_number() over w_part as row_seq,
        ntile(5) over (partition by dept_id order by salary_rounded_avg_6_prior, id) as salary_rounded_avg_6_prior_bucket
    from enriched
    window w_part as (partition by dept_id order by salary_rounded, id)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        dept_id,
        manager_id,
        email,
        next_job_title_max_salary_rounded_int,
        prev_salary_rounded,
        row_seq,
        salary_rounded_avg_6_prior_bucket,
        round(prev_salary_rounded / salary_rounded_avg_6_prior_bucket, 2) as prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket,
        regexp_extract(email, '@(.+)$', 1) as email_domain
    from enriched_employees
    where email is not null
)
-- Grouped metrics
select
    manager_id,
    case when next_job_title_max_salary_rounded_int >= 89 then 'PREMIUM' else 'STANDARD' end as next_job_title_max_salary_rounded_int_tier,
    case when prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket >= 87 then 'HIGH' else 'LOW' end as prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket_tier,
    count(distinct id) as distinct_ids,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    max(prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket) as max_prev_salary_rounded_per_salary_rounded_avg_6_prior_bucket,
    round(avg(prev_salary_rounded), 2) as avg_prev_salary_rounded
from valid
group by 1, 2, 3
order by manager_id;
