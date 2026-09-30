with valid as (
    -- Keep recent, valid rows
    select
        id,
        event_type,
        duration_sec,
        round(duration_sec * 1.1, 2) as adjusted_duration_sec,
        concat_ws('-', page, event_type) as page_event_type_key,
        coalesce(page, 'OTHER') as page_clean
    from web_events
    where event_type = 'page_view'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by event_type order by adjusted_duration_sec desc, id) as rank_in_group
    from valid
    qualify rank_in_group = 1
),
latest_web_events as (
    -- QUALIFY-style window filter
    select *
    from latest
    qualify rank() over (partition by event_type order by rank_in_group desc) = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        event_type,
        duration_sec,
        adjusted_duration_sec,
        page_clean
    from (
        select *, row_number() over (partition by event_type order by adjusted_duration_sec desc, id) as row_num
        from latest_web_events
    ) ranked
    where row_num = 1
)
select
    id,
    event_type,
    duration_sec,
    adjusted_duration_sec,
    page_clean,
    case when event_type rlike '^(add_to_cart|search)$' then 'EXTENDED' else 'GROUP_B' end as event_type_zone
from ranked
order by event_type
