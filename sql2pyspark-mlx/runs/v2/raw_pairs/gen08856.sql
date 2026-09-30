select id, duration_sec, rank() over (order by duration_sec) as duration_sec_rank
from web_events
where page = '/search';
