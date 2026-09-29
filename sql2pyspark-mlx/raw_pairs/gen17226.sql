with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, substring(status, 1, 3) as status_prefix
    from subscriptions
    where customer_id is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by customer_id order by end_date desc) as dedup_rank
    from filtered
    qualify dedup_rank = 1
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from deduped e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.end_date between sub.start_date and sub.end_date
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id_2,
        sub.plan as subscription_plan_2,
        sub.monthly_fee as subscription_monthly_fee_2
    from attributed a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.end_date >= sub.start_date
       and a.end_date <= date_add(sub.end_date, 3)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        subscription_id,
        subscription_id_2,
        status,
        subscription_monthly_fee_2,
        subscription_monthly_fee
    from matched
) src
unpivot include nulls (
    reading for metric_name in (subscription_monthly_fee_2, subscription_monthly_fee)
)
