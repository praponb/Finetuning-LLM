with matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from subscriptions src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.start_date >= sub.start_date
       and src.start_date < sub.end_date + interval 7 days
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        d.country,
        d.city
    from matched a
    left join customers d
        on a.customer_id <=> d.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        plan,
        start_date,
        end_date,
        status,
        subscription_id,
        subscription_monthly_fee,
        country,
        min(subscription_monthly_fee) over w_running as last4_min_subscription_monthly_fee,
        avg(subscription_monthly_fee) over w_running as last4_avg_subscription_monthly_fee,
        count(subscription_monthly_fee) over w_running as last4_count_subscription_monthly_fee,
        round(subscription_monthly_fee / sum(subscription_monthly_fee) over (partition by city), 4) as subscription_monthly_fee_share
    from joined
    window w_running as (partition by country order by end_date, id, subscription_id rows between 3 preceding and current row)
),
remaining_keys as (
    -- Set intersection on keys
    select plan
    from scored
    intersect
    select plan
    from subscriptions
    where start_date between '2023-11-01' and '2024-02-18'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.plan = k.plan
),
enriched as (
    -- Ranking and gap features
    select
        plan,
        start_date,
        status,
        last4_min_subscription_monthly_fee,
        last4_avg_subscription_monthly_fee,
        subscription_monthly_fee_share,
        percent_rank() over (partition by plan order by subscription_monthly_fee_share nulls first) as subscription_monthly_fee_share_percent_rank
    from reconciled
),
enriched_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by month(start_date) order by subscription_monthly_fee_share_percent_rank) as subscription_monthly_fee_share_percent_rank_rank,
        percent_rank() over (partition by status, plan order by last4_avg_subscription_monthly_fee desc) as last4_avg_subscription_monthly_fee_percent_rank,
        count(subscription_monthly_fee_share) over (partition by status, plan) as status_count_subscription_monthly_fee_share
    from enriched
)
select
    plan,
    last4_min_subscription_monthly_fee,
    last4_avg_subscription_monthly_fee,
    subscription_monthly_fee_share,
    subscription_monthly_fee_share_percent_rank,
    subscription_monthly_fee_share_percent_rank_rank,
    last4_avg_subscription_monthly_fee_percent_rank
from enriched_subscriptions;
