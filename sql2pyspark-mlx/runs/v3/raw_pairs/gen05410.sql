SELECT DATE_TRUNC('MONTH', s.started_at) started_month, MIN(we.event_time) min_event_time
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.event_type IN ('add_to_cart', 'page_view')
GROUP BY DATE_TRUNC('MONTH', s.started_at)
ORDER BY started_month DESC, min_event_time;
