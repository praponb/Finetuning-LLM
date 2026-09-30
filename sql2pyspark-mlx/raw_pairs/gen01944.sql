WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', browser, device) AS browser_device_key
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-09-20 05:00:00')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM islands s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page IS NOT NULL
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        browser,
        STDDEV_SAMP(session_id) OVER (PARTITION BY browser) AS browser_stddev_samp_session_id,
        MIN(session_id) OVER (PARTITION BY browser) AS browser_min_session_id
    FROM not_flagged
)
SELECT id, browser
FROM metrics;
