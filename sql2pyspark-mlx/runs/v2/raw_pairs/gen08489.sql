select we.id, we.session_id, we.page
from web_events we
where we.session_id in (select id from sessions where browser = 'Safari')
