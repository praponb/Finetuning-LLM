select
    *,
    percent_rank() over (partition by session_id order by duration_sec desc nulls first) duration_sec_percent_rank,
    last_value(page, true) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) last_known_page,
    last_value(duration_sec) ignore nulls over w_hist last_known_duration_sec
from web_events
window w_hist as (partition by event_type, session_id order by event_time, id rows between unbounded preceding and current row);
