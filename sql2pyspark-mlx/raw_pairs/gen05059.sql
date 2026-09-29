with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        device,
        browser,
        country,
        case when device in ('tablet', 'mobile') then 'Tier 1' else 'Other' end as device_group,
        coalesce(browser, 'NONE') as resolved_browser
    from sessions
    where customer_id is not null
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(device) as clean_device,
        substring(device, 1, 4) as device_prefix,
        coalesce(browser, 'UNKNOWN') as resolved_browser_2
    from filtered
    where browser is not null
      and customer_id = 28
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.device,
        s.browser,
        s.device_group,
        s.resolved_browser,
        s.clean_device,
        s.device_prefix,
        s.resolved_browser_2,
        top1.web_event_event_time,
        top1.web_event_duration_sec
    from cleaned s
    left join lateral (
            select c.event_time as web_event_event_time, c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when web_event_event_time - lag(web_event_event_time) over (partition by browser order by web_event_event_time, id) > interval 7 days then 1 else 0 end as session_start_flag
    from latest_match
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        device,
        browser,
        device_prefix,
        web_event_event_time,
        web_event_duration_sec,
        sum(session_start_flag) over (partition by browser order by web_event_event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from sessions_sessions s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.duration_sec > 456
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.session_id in (16, 24, 26)
        )
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        device,
        web_event_duration_sec,
        burst_id,
        trim(device) as clean_device,
        coalesce(browser, 'DIRECT') as browser_clean
    from kept
    where browser is distinct from 'Edge'
      and web_event_event_time < '2025-02-01'
)
-- Multi-level aggregation
select
    case when burst_id >= 7 then 'PREMIUM' else 'STANDARD' end as burst_id_tier,
    count(case when device = 'desktop' then 1 end) as desktop_count,
    count(distinct device) as distinct_devices,
    percentile_approx(burst_id, 0.9) as burst_id_p90,
    sort_array(collect_set(device)) as device_list,
    sort_array(collect_set(device)) as device_list_2
from base
group by 1;
