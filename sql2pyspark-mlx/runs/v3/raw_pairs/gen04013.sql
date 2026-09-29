select event_type, round(avg(duration_sec), 2) as avg_duration_sec, count(duration_sec) as duration_sec_count from web_events where event_type = 'click' group by 1 having avg_duration_sec >= 44;
