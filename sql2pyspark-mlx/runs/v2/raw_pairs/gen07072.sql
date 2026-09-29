select s.id, s.browser, t.row_count
from sessions s
inner join (select session_id, count(*) as row_count
    from web_events
    group by session_id) t on s.id = t.session_id
where t.row_count >= 9;
