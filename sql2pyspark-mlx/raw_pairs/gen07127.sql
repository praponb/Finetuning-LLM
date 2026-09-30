WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(device) AS device_length
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-09-15 16:00:00')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        started_at,
        device_length
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY device ORDER BY started_at DESC) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(device_length) OVER (PARTITION BY customer_id) AS customer_id_avg_device_length,
        LAST_VALUE(device_length, TRUE) OVER (PARTITION BY customer_id ORDER BY device_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device_length
    FROM latest
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(customer_id_avg_device_length, 0.5) AS customer_id_avg_device_length_p50,
    ROUND(AVG(last_known_device_length), 2) AS avg_last_known_device_length,
    MIN(started_at) AS first_started_at
FROM kept
GROUP BY year_start
HAVING avg_last_known_device_length >= 0
