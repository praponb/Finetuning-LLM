with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        concat_ws('/', page, event_type) as page_event_type_key
    from web_events
    where event_time > to_date('2024-02-19')
      and page is not null
      and hour(event_time) between 10 and 17
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(page) as page_length,
        datediff(current_date(), event_time) as days_since_event_time,
        coalesce(page, 'NONE') as page_clean
    from filtered
    where event_time > to_date('2023-03-31')
      and page is not null
      and hour(event_time) between 9 and 13
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        page_event_type_key,
        page_clean,
        trim(page) as clean_page,
        coalesce(page, 'DIRECT') as resolved_page
    from valid
    where page is not null
      and session_id in (21, 30)
),
enriched as (
    -- Attach sessions attributes
    select
        s.*,
        b.browser,
        b.device
    from cleaned s
    left join sessions b
        on s.session_id = b.id
)
-- Aggregate with conditional metrics
select
    device,
    count(*) as event_count,
    size(collect_set(device)) as num_devices
from enriched
group by device;
