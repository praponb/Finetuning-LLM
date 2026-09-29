select s.device, we.page from sessions s left join web_events we on s.id = we.session_id order by page
