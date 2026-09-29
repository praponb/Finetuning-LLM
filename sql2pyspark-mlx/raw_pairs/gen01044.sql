WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', device, browser) AS device_browser_key
    FROM sessions
    WHERE started_at > DATE '2023-06-17'
      AND customer_id IS NOT NULL
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_time,
        latest.web_event_page
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.event_time, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM with_last
    EXCEPT
    SELECT customer_id, id
    FROM sessions
    WHERE customer_id IN (1, 30)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (8, 19, 30)
        )
)
-- Aggregate with conditional metrics
SELECT
    device,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM kept
GROUP BY 1
ORDER BY device DESC
