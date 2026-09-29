with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time asc, id asc) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time asc, id asc) + interval 60 days then 1 else 0 end as new_group_flag
    from web_events
)
-- Running sum of the flag = session number
select
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    sum(new_group_flag) over (partition by event_type order by event_time asc, id asc rows between unbounded preceding and current row) as burst_id
from gaps;
