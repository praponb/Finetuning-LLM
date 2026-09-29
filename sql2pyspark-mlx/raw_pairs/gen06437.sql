with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        case when country in ('SG', 'JP') then 'Group A' else 'Other' end as country_group,
        trim(browser) as clean_browser,
        coalesce(browser, 'UNKNOWN') as browser_clean
    from sessions
    where started_at >= timestamp '2023-04-14 08:00:00'
      and browser is not null
),
matched_keys as (
    -- Set difference on keys
    select customer_id, device
    from cleaned
    except all
    select customer_id, device
    from sessions
    where browser is not null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.customer_id = k.customer_id and s.device = k.device
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('YEAR', started_at) as started_at_period,
        coalesce(browser, 'NONE') as browser_clean_2
    from verified
    where started_at > date '2023-06-24'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_type = 'page_view'
        )
),
summary as (
    -- Multi-level aggregation
    select
        country,
        sort_array(collect_set(device)) as device_list,
        max(started_at) as last_started_at,
        (unix_timestamp(max(started_at_period)) - unix_timestamp(min(started_at_period))) / 60 as span_minutes,
        count(case when device = 'desktop' then 1 end) as desktop_count,
        size(collect_set(browser)) as num_browsers
    from not_flagged
    group by country
)
select
    country,
    device_list,
    last_started_at,
    span_minutes,
    num_browsers,
    case when country rlike '^(JP|TH)$' then 'GROUP_A' when country rlike '^(US|SG)$' then 'OTHER_ZONE' else 'PRIORITY' end as country_zone
from summary;
