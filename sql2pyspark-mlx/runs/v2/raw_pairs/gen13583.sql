select id, page, count(*) over (partition by page) as page_count
from web_events
order by id;
