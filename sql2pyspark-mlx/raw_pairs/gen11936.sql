with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        cast(started_at as date) as started_at_day
    from sessions
    where browser = 'Safari'
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by customer_id order by started_at_day desc, id) = 1
),
matched_keys as (
    -- Set difference on keys
    select customer_id, browser
    from deduped
    except all
    select customer_id, browser
    from sessions
    where country = 'DE'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join matched_keys k on s.customer_id = k.customer_id and s.browser = k.browser
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time <= s.started_at
        )
),
filtered as (
    -- Basic cleansing and defaults
    select
        device,
        started_at,
        country,
        day(started_at) as started_at_day_2,
        case when device in ('tablet', 'desktop') then 'Priority' else 'Other' end as device_group
    from not_flagged
    where country is not null
      and browser is distinct from 'Edge'
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by country order by started_at_day_2 desc nulls first) as started_at_day_2_dense_rank,
        avg(started_at_day_2) over (partition by device, country) as device_avg_started_at_day_2
    from filtered
)
select
    device,
    started_at,
    country,
    started_at_day_2,
    device_group,
    started_at_day_2_dense_rank
from scored
