select id, session_id, duration_sec, percent_rank() over (partition by session_id order by duration_sec) as duration_sec_pct_rank from web_events
