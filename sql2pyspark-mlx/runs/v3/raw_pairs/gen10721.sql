select page, count(case when page = '/home' then 1 end) as home_count, max(event_time) as last_event_time from web_events group by page having count(case when page = '/home' then 1 end) > 1;
