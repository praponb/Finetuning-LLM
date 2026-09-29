WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('YEAR', started_at) AS started_at_period
    FROM sessions
    WHERE started_at > TO_DATE('2023-07-20')
      AND browser IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at_period - LAG(started_at_period) OVER (PARTITION BY customer_id ORDER BY started_at_period, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        browser,
        started_at_period,
        SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', started_at_period) AS week_start,
    session_id,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    MAX(started_at_period) AS last_started_at_period
FROM sessions_sessions
GROUP BY DATE_TRUNC('WEEK', started_at_period), session_id;
