select
    s.browser,
    s.country,
    src.event_type,
    count(*) as rows_total,
    min(src.duration_sec) as min_duration_sec
from web_events src
left join sessions s on src.session_id = s.id
group by s.browser, s.country, src.event_type;
