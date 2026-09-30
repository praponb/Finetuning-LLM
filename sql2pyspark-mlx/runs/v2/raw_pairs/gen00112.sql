select session_id, concat_ws(', ', sort_array(collect_set(page))) as page_values
from web_events
where duration_sec is not null and page is null and page in ('/search', '/cart')
group by session_id
order by page_values
