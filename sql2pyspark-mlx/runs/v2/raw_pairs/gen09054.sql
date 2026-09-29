select id, page, duration_sec, max(duration_sec) over (partition by page) as page_max_duration_sec, row_number() over (partition by page order by duration_sec desc, id) as row_num from web_events;
