with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        started_at,
        lower(device) as device_lower,
        case when browser in ('Safari', 'Firefox') then 'Core' else 'Other' end as browser_group
    from sessions
    where browser in ('Chrome', 'Safari')
      and country is not null
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval 90 days then 1 else 0 end as is_new_session
    from filtered
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        device,
        started_at,
        device_lower,
        browser_group,
        sum(is_new_session) over (partition by device order by started_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
latest as (
    -- QUALIFY-style window filter
    select *
    from sessions_sessions
    qualify rank() over (partition by device, customer_id order by started_at desc) = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        customer_id,
        device,
        burst_id
    from (
        select *, row_number() over (partition by device, customer_id order by burst_id desc, id) as rank_in_group
        from latest
    ) t
    where rank_in_group = 1
)
select
    id,
    customer_id,
    device
from ranked
order by id
