WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        LENGTH(browser) AS browser_length,
        COALESCE(browser, 'DIRECT') AS browser_filled
    FROM sessions
    WHERE country NOT IN ('US', 'JP')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(browser_length) OVER w_grp AS next_browser_length,
        LAG(browser_length) OVER w_grp AS prev_browser_length,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM base
    WHERE customer_id IS NULL
    WINDOW w_grp AS (PARTITION BY browser ORDER BY browser_length, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec <= 305
        )
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM not_flagged
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    browser,
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    MAX(started_at) AS last_started_at
FROM sessionized
GROUP BY CUBE (browser, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY span_minutes DESC NULLS LAST;
