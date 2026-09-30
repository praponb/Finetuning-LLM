WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        INITCAP(TRIM(device)) AS device_title,
        COALESCE(browser, 'unassigned') AS browser_filled
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-05-27 05:00:00'
      AND customer_id IS NOT NULL
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM filtered s
WHERE EXISTS (
        SELECT 1
        FROM web_events x
        WHERE x.session_id = s.id
          AND x.event_time <= s.started_at
    );
