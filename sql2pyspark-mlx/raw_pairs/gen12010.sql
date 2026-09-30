select s.browser, concat_ws(', ', sort_array(collect_set(we.event_type))) as event_type_values, max(we.event_time) as latest_event_time
from web_events we
left join sessions s on we.session_id = s.id
where we.page in ('/home', '/search', '/cart')
group by s.browser
order by browser;
