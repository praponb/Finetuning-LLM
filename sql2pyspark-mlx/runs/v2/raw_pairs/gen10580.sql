select s.browser, we.session_id
from sessions s
inner join web_events we on we.session_id = s.id
