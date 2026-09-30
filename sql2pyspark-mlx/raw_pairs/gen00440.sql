select s.browser, t.duration_sec_count from sessions s join (select session_id, count(duration_sec) duration_sec_count from web_events group by session_id) t on s.id = t.session_id
