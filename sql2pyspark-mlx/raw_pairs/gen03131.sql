with enriched as (
    -- Ranking and gap features
    select
        *,
        count(duration_sec) over (partition by session_id, page order by duration_sec, id rows between unbounded preceding and current row) as running_count_duration_sec,
        avg(duration_sec) over (partition by session_id, page order by duration_sec, id rows between unbounded preceding and current row) as running_avg_duration_sec,
        min(duration_sec) over (partition by session_id, page order by duration_sec, id rows between unbounded preceding and current row) as running_min_duration_sec,
        sum(duration_sec) over (partition by session_id, page order by duration_sec, id rows between unbounded preceding and current row) as running_sum_duration_sec
    from web_events
)
select
    id,
    event_type,
    page,
    running_count_duration_sec,
    running_avg_duration_sec
from enriched
order by page desc nulls first;
