with valid_subscriptions as (
    select *
    from subscriptions
    where end_date > '2023-08-07' and plan not in ('standard', 'enterprise', 'premium')
),
plan_summary as (
    select
        plan,
        date_trunc('MONTH', start_date) as start_month,
        count(monthly_fee) as monthly_fee_count,
        round((100 * sum(case when plan = 'enterprise' then 1 else 0 end)) / count(*), 1) as pct_enterprise
    from valid_subscriptions
    group by plan, date_trunc('MONTH', start_date)
)
select *
from plan_summary
where monthly_fee_count > 8
order by pct_enterprise desc;
