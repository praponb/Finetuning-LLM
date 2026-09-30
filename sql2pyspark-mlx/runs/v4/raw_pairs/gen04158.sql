WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.credit_limit,
        c.segment,
        c.city
    FROM sessionized s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM joined
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_tier,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
        (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
        COUNT_IF(browser = 'Chrome') AS chrome_events
    FROM sessions_sessions
    GROUP BY 1, 2
)
SELECT
    device,
    session_no_tier,
    browser_list,
    span_minutes,
    chrome_events
FROM daily;
