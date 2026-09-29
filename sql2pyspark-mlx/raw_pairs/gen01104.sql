with valid as (
    select
        *,
        initcap(trim(location)) as location_title,
        coalesce(location, 'NONE') as location_clean
    from departments
    where dept_name in ('HR', 'Finance')
),
eligible as (
    select s.*
    from valid s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.full_name like '%bob%'
        )
),
metrics as (
    select
        id,
        location,
        location_title,
        percent_rank() over (partition by location order by budget asc nulls first) as budget_percent_rank,
        avg(budget) over (partition by dept_name, location order by budget asc, id asc rows between 5 preceding and 1 preceding) as budget_avg_5_prior
    from eligible
    where budget <= 1497000
),
base as (
    select
        id,
        location,
        budget_percent_rank,
        budget_avg_5_prior,
        trim(location) as clean_location,
        coalesce(location, 'DIRECT') as location_filled
    from metrics
    where location in ('Singapore', 'Tokyo', 'Berlin')
),
metrics_departments as (
    select
        id,
        location,
        clean_location,
        cume_dist() over (partition by location order by budget_avg_5_prior asc nulls last) as budget_avg_5_prior_cume_dist,
        ntile(10) over w_ordered as budget_avg_5_prior_bucket
    from base
    window w_ordered as (partition by location order by budget_avg_5_prior asc, id asc)
),
cleaned as (
    select *, trim(location) as clean_location_2
    from metrics_departments
    where location is not null
)
select
    location,
    case when budget_avg_5_prior_cume_dist >= 0.66 then 'HIGH' else 'LOW' end as budget_avg_5_prior_cume_dist_tier,
    sum(case when location = 'Bangkok' then budget_avg_5_prior_cume_dist else 0 end) as bangkok_budget_avg_5_prior_cume_dist,
    count(case when location = 'Berlin' then 1 end) as berlin_count,
    round(avg(budget_avg_5_prior_bucket), 2) as avg_budget_avg_5_prior_bucket,
    avg(budget_avg_5_prior_cume_dist) as avg_budget_avg_5_prior_cume_dist,
    round(sum(budget_avg_5_prior_cume_dist) / nullif(count(case when location = 'London' then 1 end), 0), 2) as budget_avg_5_prior_cume_dist_per_london,
    count(case when location = 'Singapore' then 1 end) as singapore_count,
    percent_rank() over (partition by location order by avg(budget_avg_5_prior_cume_dist) desc) as avg_budget_avg_5_prior_cume_dist_percent_rank
from cleaned
group by location, budget_avg_5_prior_cume_dist_tier
having avg(budget_avg_5_prior_cume_dist) > 0;
