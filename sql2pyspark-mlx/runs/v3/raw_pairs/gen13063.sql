select event_type, page, count(distinct duration_sec) as unique_pages from web_events where session_id in (30, 38) group by 1, 2 order by unique_pages desc;
