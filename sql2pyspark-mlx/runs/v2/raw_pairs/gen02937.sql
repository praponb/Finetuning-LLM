select *, case when page in ('/home', '/search') then 'Group A' else 'Other' end as page_group from web_events where event_time < '2025-02-25' and session_id = 31
