WITH enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.country AS customer_country,
        b.city,
        b.age
    FROM sessions src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.segment IN ('Corporate', 'Consumer')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_country ORDER BY started_at, id) AS prev_started_at
    FROM not_flagged
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        session_start_flag,
        MAX(age) OVER w_hist AS running_max_age,
        SUM(age) OVER w_hist AS running_sum_age,
        MIN(age) OVER w_hist AS running_min_age,
        NTILE(5) OVER (PARTITION BY customer_country ORDER BY age, id) AS age_bucket
    FROM sessions_sessions
    WINDOW w_hist AS (PARTITION BY city ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_min_age) OVER (PARTITION BY browser ORDER BY running_sum_age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_running_min_age,
        MIN(running_min_age) OVER (PARTITION BY browser ORDER BY running_sum_age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_running_min_age,
        SUM(running_min_age) OVER (PARTITION BY browser ORDER BY running_sum_age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_running_min_age,
        CUME_DIST() OVER (PARTITION BY customer_id, device ORDER BY session_start_flag DESC NULLS LAST) AS session_start_flag_cume_dist
    FROM scored
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        browser,
        DATE_TRUNC('YEAR', started_at) AS year_start,
        SUM(last4_avg_running_min_age) AS total_last4_avg_running_min_age,
        SIZE(COLLECT_SET(device)) AS num_devices,
        MAX(last4_min_running_min_age) AS max_last4_min_running_min_age,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes
    FROM enriched_sessions
    GROUP BY browser, DATE_TRUNC('YEAR', started_at)
)
SELECT
    browser,
    year_start,
    total_last4_avg_running_min_age,
    max_last4_min_running_min_age,
    span_minutes
FROM grouped
ORDER BY total_last4_avg_running_min_age NULLS FIRST, span_minutes;
