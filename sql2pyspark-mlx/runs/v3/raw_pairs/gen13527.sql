with base as (
    select *
    from subscriptions
    where year(start_date) = 2025
),
summary as (
    select plan, count(distinct monthly_fee) as unique_plans, count(monthly_fee) as monthly_fee_count
    from base
    group by plan
)
select *
from summary
where unique_plans >= 7
order by unique_plans;
