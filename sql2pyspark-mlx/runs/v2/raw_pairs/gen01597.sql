select
    id,
    duration_sec,
    row_number() over (order by duration_sec, id) as row_num,
    percent_rank() over (order by duration_sec) as duration_sec_pct_rank
from web_events
where duration_sec <= 257
