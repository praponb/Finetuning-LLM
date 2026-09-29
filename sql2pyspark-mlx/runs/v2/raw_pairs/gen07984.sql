select id, session_id, duration_sec
from (
    select *, row_number() over (partition by session_id order by duration_sec desc, id) as rnk
    from web_events
) as r
where rnk = 1;
