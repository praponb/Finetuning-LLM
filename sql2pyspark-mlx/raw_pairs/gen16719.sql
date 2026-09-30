with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        case when country in ('JP', 'DE') then 'Tier 1' else 'Other' end as country_group
    from sessions
    where started_at > to_timestamp('2023-05-30 17:00:00')
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from valid src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.started_at >= sub.start_date
       and src.started_at < sub.end_date + interval 30 days
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_window s
    where exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.started_at
        )
),
cleaned as (
    -- Basic cleansing and defaults
    select *, upper(plan) as plan_upper
    from not_flagged
    where browser in ('Edge', 'Chrome', 'Firefox')
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.device,
        s.started_at,
        s.country_group,
        s.subscription_id,
        s.monthly_fee,
        s.plan_upper,
        x.event_type
    from cleaned s
    left join lateral (
            select c.event_type
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    customer_id,
    device,
    started_at,
    country_group,
    subscription_id,
    monthly_fee,
    plan_upper,
    event_type
from with_last
order by started_at nulls first;
