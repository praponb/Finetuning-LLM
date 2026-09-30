select id, page, duration_sec, sum(duration_sec) over (partition by page) as page_sum_duration_sec from web_events
