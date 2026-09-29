with base as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        monthly_fee,
        case when status = 'ACTIVE' then 1 else 0 end as is_active,
        round(monthly_fee * 1.07, 2) as monthly_fee_scaled
    from subscriptions
    where customer_id is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        round(is_active, 1) as is_active_rounded,
        case when plan in ('basic', 'enterprise') then 'Group A' else 'Other' end as plan_group
    from base
    where monthly_fee < 240
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by plan order by monthly_fee_scaled desc nulls first, id) as latest_rank
        from filtered
    ) ranked
    where latest_rank = 1
),
base_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select *, case when monthly_fee >= 255 then 'High' when monthly_fee >= 116 then 'Medium' else 'Low' end as monthly_fee_band
    from ranked
    where monthly_fee_scaled >= 25
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        plan,
        monthly_fee,
        is_active,
        plan_group,
        latest_rank,
        monthly_fee_band,
        ntile(10) over (partition by plan order by is_active desc, id) as is_active_bucket,
        ntile(4) over (order by monthly_fee, id) as monthly_fee_bucket,
        row_number() over (partition by plan order by latest_rank, id) as event_seq,
        ntile(4) over (partition by plan order by monthly_fee_scaled desc, id) as monthly_fee_scaled_bucket,
        last_value(monthly_fee, true) over (partition by plan order by latest_rank, id rows between unbounded preceding and current row) as last_known_monthly_fee
    from base_subscriptions
    where monthly_fee between 140 and 200
),
ranked_subscriptions as (
    -- QUALIFY-style window filter
    select
        id,
        plan,
        is_active,
        latest_rank,
        monthly_fee_band,
        is_active_bucket,
        monthly_fee_bucket,
        event_seq,
        monthly_fee_scaled_bucket
    from (
        select *, row_number() over (partition by plan order by last_known_monthly_fee desc, id) as row_num
        from with_windows
    ) t
    where row_num = 1
)
select
    id,
    plan,
    is_active,
    latest_rank,
    monthly_fee_band,
    is_active_bucket,
    monthly_fee_bucket,
    monthly_fee_scaled_bucket,
    case when plan rlike '^(basic|premium)$' then 'OTHER_ZONE' else 'EXTENDED' end as plan_zone
from ranked_subscriptions
