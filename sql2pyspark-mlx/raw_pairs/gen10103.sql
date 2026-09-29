with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        upper(country) as country_upper,
        coalesce(country, 'UNKNOWN') as resolved_country
    from sessions
    where browser is distinct from 'Edge'
      and to_date(started_at) >= '2025-10-01'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time <= s.started_at
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.page in ('/product', '/cart')
        )
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from not_flagged e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.started_at >= sub.start_date
       and e.started_at <= date_add(sub.end_date, 10)
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by device order by started_at, id, subscription_id) > interval '60' day then 1 else 0 end as is_new_burst
    from with_window
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        device,
        browser,
        started_at,
        country_upper,
        subscription_id,
        sum(is_new_burst) over (partition by device order by started_at, id, subscription_id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from sessions_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time >= s.started_at
        )
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            browser,
            device,
            island_id
        from eligible
    ) src
    pivot (
        sum(island_id) as total
        for device in ('desktop' as desktop, 'mobile' as mobile)
    )
)
select
    browser,
    desktop,
    case when browser rlike '^(Edge|Safari)$' then 'GROUP_A' when browser rlike '^(Chrome|Firefox)$' then 'CORE' else 'OTHER_ZONE' end as browser_zone
from by_type
order by desktop nulls first;
