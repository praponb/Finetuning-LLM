with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        concat(page, ' - ', event_type) as page_event_type,
        trim(event_type) as clean_event_type
    from web_events
    where event_time >= date '2023-06-28'
      and page is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval '90' day then 1 else 0 end as is_new_session
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from flagged
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval '90' day then 1 else 0 end as is_new_burst
    from islands
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        page_event_type,
        clean_event_type,
        is_new_session,
        sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_3
    from flagged_2
),
kept_keys as (
    -- Set difference on keys
    select session_id, event_type
    from sessionized
    except distinct
    select session_id, event_type
    from web_events
    where page = '/cart'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessionized s
    inner join kept_keys k on s.session_id = k.session_id and s.event_type = k.event_type
)
select
    s.session_id,
    s.event_type,
    s.page,
    s.page_event_type,
    s.is_new_session,
    s.session_id_3,
    (select avg(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as avg_web_event_duration_sec
from verified s
order by avg_web_event_duration_sec desc nulls last, session_id
