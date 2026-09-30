with prev_events as (
    select *, lag(event_time) over (partition by event_type order by event_time, id) as prev_event_time
    from web_events
),
gaps as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '90' day then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_web_events as (
    select *, sum(session_start_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
select
    event_type,
    date_trunc('week', prev_event_time) as week_start,
    size(collect_set(event_type)) as num_event_types,
    max(duration_sec) filter (where page = '/checkout') as checkout_max_duration_sec,
    round(sum(burst_id) / nullif(count(case when page = '/checkout' then 1 end), 0), 2) as burst_id_per_checkout,
    max(burst_id) filter (where event_type = 'search') as search_max_burst_id,
    approx_percentile(burst_id, 0.5) as burst_id_p50,
    size(collect_set(page)) as num_pages
from sessions_web_events
group by event_type, week_start
