select * from (select page, max(duration_sec) - min(duration_sec) duration_sec_range, max(event_time) last_event_time from web_events group by page) agg where duration_sec_range >= 59
