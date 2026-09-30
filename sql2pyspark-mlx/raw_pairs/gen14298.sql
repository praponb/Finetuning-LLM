with valid as (
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        initcap(trim(page)) as page_title,
        initcap(trim(page)) as page_title_2,
        coalesce(page, 'OTHER') as page_clean
    from web_events
    where event_time >= date '2023-04-08'
),
deduped as (
    select *, row_number() over (partition by session_id order by duration_sec desc, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
scored as (
    select
        id,
        event_type,
        duration_sec,
        page_title,
        page_title_2,
        min(duration_sec) over (partition by session_id, event_type) as session_id_min_duration_sec,
        lead(duration_sec, 1) over (partition by session_id order by latest_rank, id) as next_duration_sec
    from deduped
    where duration_sec >= 446
),
kept_keys as (
    select id, event_type
    from scored
    intersect
    select id, event_type
    from web_events
    where to_date(event_time) >= '2023-06-01'
),
verified as (
    select s.*
    from scored s
    inner join kept_keys k on s.id = k.id and s.event_type = k.event_type
),
enriched as (
    select
        *,
        cume_dist() over w_running as next_duration_sec_cume_dist,
        rank() over (partition by event_type order by next_duration_sec nulls first) as next_duration_sec_rank,
        avg(next_duration_sec) over w_running_2 as event_type_avg_next_duration_sec,
        dense_rank() over w_recent as session_id_min_duration_sec_dense_rank,
        stddev_samp(duration_sec) over w_running_2 as event_type_stddev_samp_duration_sec
    from verified
    window w_running as (order by next_duration_sec), w_running_2 as (partition by event_type), w_recent as (partition by event_type order by session_id_min_duration_sec)
),
scored_web_events as (
    select
        id,
        event_type,
        page_title_2,
        session_id_min_duration_sec,
        next_duration_sec_cume_dist,
        next_duration_sec_rank,
        session_id_min_duration_sec_dense_rank,
        event_type_stddev_samp_duration_sec,
        percent_rank() over (order by event_type_avg_next_duration_sec desc) as event_type_avg_next_duration_sec_percent_rank,
        stddev_samp(event_type_stddev_samp_duration_sec) over (partition by event_type) as event_type_stddev_samp_event_type_stddev_samp_duration_sec
    from enriched
),
filtered as (
    select
        *,
        round(next_duration_sec_cume_dist, 0) as next_duration_sec_cume_dist_rounded,
        trim(event_type) as clean_event_type
    from scored_web_events
    where next_duration_sec_rank < 3
)
select
    id,
    event_type,
    page_title_2,
    session_id_min_duration_sec,
    event_type_avg_next_duration_sec_percent_rank,
    event_type_stddev_samp_event_type_stddev_samp_duration_sec,
    next_duration_sec_cume_dist_rounded,
    clean_event_type
from filtered
order by next_duration_sec_cume_dist_rounded desc nulls last, event_type_avg_next_duration_sec_percent_rank;
