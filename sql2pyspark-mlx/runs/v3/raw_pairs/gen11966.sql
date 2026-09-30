select
    s.id,
    s.browser,
    t.web_event_count,
    t.event_type_list
from sessions s
left join (select session_id, count(*) as web_event_count, sort_array(collect_set(event_type)) as event_type_list
    from web_events
    group by session_id) t on s.id = t.session_id
where t.web_event_count > 4
order by id desc;
