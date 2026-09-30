SELECT s.id, s.customer_id FROM sessions s WHERE s.customer_id IS NULL AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type IN ('purchase', 'click', 'page_view'));
