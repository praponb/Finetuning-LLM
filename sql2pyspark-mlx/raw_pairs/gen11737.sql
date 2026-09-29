with ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by customer_id order by end_date desc nulls last, id) as rank_in_group
        from subscriptions
    ) t
    where rank_in_group = 1
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from ranked a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.end_date between sub.start_date and sub.end_date
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        round(monthly_fee / sum(monthly_fee) over (partition by month(end_date), subscription_plan), 4) as monthly_fee_share,
        avg(monthly_fee) over (partition by month(end_date), status order by subscription_id, id, subscription_id rows between 2 preceding and 1 preceding) as monthly_fee_avg_2_prior,
        first_value(subscription_monthly_fee) ignore nulls over (partition by status order by start_date, id, subscription_id rows between unbounded preceding and current row) as first_known_subscription_monthly_fee
    from attributed
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(monthly_fee_avg_2_prior) over (partition by subscription_plan, plan order by start_date, id, subscription_id rows between unbounded preceding and current row) as running_max_monthly_fee_avg_2_prior,
        count(monthly_fee_avg_2_prior) over (partition by subscription_plan, plan order by start_date, id, subscription_id rows between unbounded preceding and current row) as running_count_monthly_fee_avg_2_prior,
        avg(monthly_fee_avg_2_prior) over (partition by subscription_plan, plan order by start_date, id, subscription_id rows between unbounded preceding and current row) as running_avg_monthly_fee_avg_2_prior
    from with_windows
)
select
    id,
    customer_id,
    plan,
    start_date,
    monthly_fee,
    status,
    rank_in_group,
    subscription_id,
    subscription_plan,
    case when plan rlike '^(enterprise|premium)$' then 'GROUP_A' when plan rlike '^(basic|standard)$' then 'PRIORITY' else 'GROUP_B' end as plan_zone
from scored
order by rank_in_group nulls first;
