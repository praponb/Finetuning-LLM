select sessions.browser, count(web_events.id) web_event_count, round(avg(web_events.duration_sec), 2) avg_duration_sec
from sessions
left outer join web_events on web_events.session_id = sessions.id
where sessions.browser not in ('Safari', 'Chrome')
group by sessions.browser
order by avg_duration_sec desc;
