SELECT we.id, we.event_type, we.page, we.event_time FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE device IN ('desktop', 'mobile', 'tablet')) ORDER BY id;
