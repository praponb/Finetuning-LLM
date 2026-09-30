select
    *,
    count(duration_sec) over (partition by page) as page_count_duration_sec,
    avg(duration_sec) over (partition by session_id order by duration_sec, id rows between 2 preceding and 1 preceding) as duration_sec_avg_2_prior,
    avg(duration_sec) over w_grp as duration_sec_avg_5
from web_events
where session_id in (2, 5, 39)
window w_grp as (partition by session_id order by duration_sec, id rows between 5 preceding and current row);
