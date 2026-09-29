with with_gaps as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '90' day then 1 else 0 end as is_new_burst
    from web_events
),
sessions_web_events as (
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps
)
select
    date_trunc('week', event_time) as week_start,
    date_trunc('year', event_time) as year_start,
    round(avg(duration_sec), 2) as avg_duration_sec,
    count(distinct id) as distinct_ids,
    count_if(page = '/cart') as cart_events,
    grouping_id() as grouping_level
from sessions_web_events
group by rollup (date_trunc('week', event_time), date_trunc('year', event_time))
having cart_events > 1
order by distinct_ids desc nulls first;
