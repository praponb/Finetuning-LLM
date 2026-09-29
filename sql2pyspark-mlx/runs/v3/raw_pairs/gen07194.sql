select session_id, case when page in ('/home', '/product') then 'core' else 'other' end as page_group, sum(duration_sec) as total_duration_sec
from web_events
where to_date(event_time) < '2023-12-01'
group by session_id, case when page in ('/home', '/product') then 'core' else 'other' end
having sum(duration_sec) < 2200
order by session_id;
