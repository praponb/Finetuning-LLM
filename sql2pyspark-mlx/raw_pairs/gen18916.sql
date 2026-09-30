with cleaned as (
    select
        *,
        substring(event_type, 1, 2) as event_type_prefix,
        coalesce(page, 'N/A') as resolved_page
    from web_events
    where event_time >= to_timestamp('2023-04-06 16:00:00')
      and page is not null
),
prev_events as (
    select *, lag(event_time) over (partition by session_id order by event_time asc, id asc) as prev_event_time
    from cleaned
),
with_gaps as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 60 days then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_web_events as (
    select *, sum(session_start_flag) over (partition by session_id order by event_time asc, id asc rows between unbounded preceding and current row) as session_id_2
    from with_gaps
),
metrics as (
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        event_type_prefix,
        session_start_flag,
        session_id_2,
        max(session_start_flag) over (partition by event_type) as event_type_max_session_start_flag,
        avg(duration_sec) over w_grp as session_id_avg_duration_sec
    from sessions_web_events
    window w_grp as (partition by session_id, event_type)
),
enriched as (
    select
        src.id,
        src.session_id,
        src.event_time,
        src.session_start_flag,
        src.event_type_max_session_start_flag,
        b.browser,
        b.device,
        b.country
    from metrics src
    join sessions b
        on src.session_id = b.id
)
select
    s.id,
    s.session_id,
    s.session_start_flag,
    s.event_type_max_session_start_flag,
    s.browser,
    s.device,
    s.country,
    (select max(t2.duration_sec) from web_events t2 where t2.session_id = s.session_id) as max_web_event_duration_sec
from enriched s
order by max_web_event_duration_sec desc, country asc
