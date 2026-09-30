WITH cleaned AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CONCAT(device, ' ', browser) AS device_browser,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-06-21 09:00:00')
      AND TO_DATE(started_at) >= '2023-11-05'
),
gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_dims AS (
    SELECT e.*, b.credit_limit
    FROM islands e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM with_dims
),
gaps_2 AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
summary AS (
    SELECT
        customer_id,
        CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
        CASE WHEN session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
        SIZE(COLLECT_SET(browser)) AS num_browsers,
        AVG(is_new_burst) AS avg_is_new_burst,
        COUNT_IF(device = 'mobile') AS mobile_events
    FROM sessionized
    GROUP BY 1, 2, 3
)
SELECT
    customer_id,
    burst_id_tier,
    session_no_tier,
    num_browsers,
    mobile_events
FROM summary
