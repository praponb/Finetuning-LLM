SELECT
    s.started_at,
    we.duration_sec,
    we.event_time,
    we.session_id,
    CEIL(we.duration_sec) duration_sec_whole
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.customer_id = 39 AND we.page <> '/product'
ORDER BY session_id DESC, s.started_at DESC
