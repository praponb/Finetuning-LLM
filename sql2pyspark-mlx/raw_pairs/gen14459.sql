with deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by customer_id order by monthly_fee, id) as rn
        from subscriptions
    ) t
    where rn = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(monthly_fee) over w_grp as prev_monthly_fee,
        row_number() over w_grp as seq_no,
        avg(monthly_fee) over (partition by status order by start_date, id rows between 3 preceding and current row) as monthly_fee_avg_3
    from deduped
    where monthly_fee < 150
    window w_grp as (partition by plan, status order by rn, id)
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from with_windows src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.end_date >= sub.start_date
       and src.end_date < sub.end_date + interval 7 days
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from with_window
    qualify row_number() over (partition by customer_id, subscription_plan order by monthly_fee_avg_3 nulls last, id, subscription_id) = 1
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    subscription_id,
    plan,
    kpi,
    reading
from (
    select
        id,
        subscription_id,
        plan,
        prev_monthly_fee,
        monthly_fee
    from ranked
) src
unpivot (
    reading for kpi in (prev_monthly_fee as PREV_MONTHLY_FEE_VAL, monthly_fee as MONTHLY_FEE)
)
