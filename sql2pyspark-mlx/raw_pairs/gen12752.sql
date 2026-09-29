with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(location) as clean_location,
        coalesce(location, 'NONE') as location_filled
    from departments
    where location is not null
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        s.clean_location,
        top1.salary,
        top1.job_title
    from base s
    join lateral (
            select c.salary, c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(job_title, 1, 3) as job_title_prefix,
        trim(location) as clean_location_2,
        coalesce(location, 'NONE') as location_clean
    from with_latest
    where dept_name is distinct from 'Marketing'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        budget,
        clean_location,
        salary,
        job_title,
        job_title_prefix,
        clean_location_2,
        rank() over (partition by dept_name, job_title order by salary desc) as salary_rank,
        dense_rank() over w_hist as salary_dense_rank,
        rank() over (order by salary nulls first) as salary_rank_2,
        avg(salary) over (order by salary, id rows between 6 preceding and 1 preceding) as salary_avg_6_prior
    from valid
    window w_hist as (partition by dept_name order by salary desc nulls first)
),
with_latest_departments as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select s.*, latest.level
    from with_windows s
    join lateral (
            select c.level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        clean_location_2,
        salary_rank,
        salary_avg_6_prior
    from (
        select *, row_number() over (partition by job_title order by salary_rank, id) as rank_in_group
        from with_latest_departments
    ) ranked
    where rank_in_group = 1
),
matched_keys as (
    -- Set difference on keys
    select id
    from deduped
    except
    select id
    from departments
    where location = 'Tokyo'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join matched_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    case when salary_rank >= 8 then 'PREMIUM' else 'STANDARD' end as salary_rank_tier,
    percentile_approx(salary_avg_6_prior, 0.5) as salary_avg_6_prior_p50,
    approx_percentile(salary_avg_6_prior, array(0.25, 0.5, 0.75)) as salary_avg_6_prior_quartiles,
    count(distinct id) as distinct_ids
from kept_rows
group by rollup (case when salary_rank >= 8 then 'PREMIUM' else 'STANDARD' end)
order by salary_avg_6_prior_p50 desc
