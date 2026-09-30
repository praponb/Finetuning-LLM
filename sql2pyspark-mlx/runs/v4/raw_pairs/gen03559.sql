WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser, country ORDER BY started_at) AS row_num
        FROM eligible
    ) t
    WHERE row_num = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, device
    FROM deduped
    EXCEPT ALL
    SELECT customer_id, device
    FROM sessions
    WHERE browser NOT IN ('Safari', 'Firefox', 'Chrome')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.device = k.device
)
-- Multi-level aggregation
SELECT
    device,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(DISTINCT browser) AS distinct_browsers,
    SIZE(COLLECT_SET(device)) AS num_devices
FROM verified
GROUP BY device, DATE_TRUNC('QUARTER', started_at)
ORDER BY num_devices DESC NULLS LAST;
