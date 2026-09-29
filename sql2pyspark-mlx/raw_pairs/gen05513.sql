with valid as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        round(duration_sec * 0.9, 2) as duration_sec_adjusted,
        coalesce(page, 'N/A') as page_filled,
        coalesce(page, 'DIRECT') as page_clean
    from web_events
    where page is not distinct from '/cart'
),
gaps as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 7 days then 1 else 0 end as is_new_session
    from valid
),
islands as (
    select
        id,
        event_type,
        page,
        event_time,
        page_filled,
        page_clean,
        sum(is_new_session) over (partition by page order by event_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
latest as (
    select *, rank() over (partition by page order by event_time desc) as rank_in_group
    from islands
    qualify rank_in_group = 1
),
flagged as (
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '90' day then 1 else 0 end as is_new_burst
    from latest
),
islands_web_events as (
    select
        id,
        event_type,
        page,
        event_time,
        page_clean,
        rank_in_group,
        sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id_2
    from flagged
)
select
    id,
    event_type,
    page,
    rank_in_group
from islands_web_events
order by rank_in_group desc;
