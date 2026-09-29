with filtered_subscriptions as (
    select *
    from subscriptions
    where start_date >= '2025-04-01'
),
status_summary as (
    select status, round(stddev(monthly_fee), 2) as stddev_monthly_fee, count(case when plan = 'standard' then 1 end) as standard_count
    from filtered_subscriptions
    group by status
)
select status, stddev_monthly_fee, standard_count
from status_summary;
