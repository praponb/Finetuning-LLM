WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        CONCAT_WS(',', browser, device) AS browser_device_key
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND browser IN ('Chrome', 'Safari')
)
-- Latest matching web_event per row (LATERAL ... LIMIT 1)
SELECT
    s.*,
    x.duration_sec,
    x.event_time
FROM filtered s
LEFT JOIN LATERAL (
        SELECT c.duration_sec, c.event_time
        FROM web_events c
        WHERE c.session_id = s.id
        ORDER BY c.duration_sec DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
