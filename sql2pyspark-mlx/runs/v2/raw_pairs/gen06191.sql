select page, round(avg(duration_sec), 2) as avg_duration_sec from web_events group by page;
