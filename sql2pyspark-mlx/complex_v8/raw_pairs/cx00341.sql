WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(days_since_opened_at) AS session_days_since_opened_at,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        MIN(session_start) AS first_session_start,
        ROUND(SUM(session_days_since_opened_at) / NULLIF(SUM(n_events), 0), 3) AS session_days_since_opened_at_to_n_events_ratio
    FROM session_summary
    WHERE agent_id IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', session_end)
    HAVING AVG(duration_min) > 72.0
)
SELECT
    session_end_month,
    first_session_start,
    session_days_since_opened_at_to_n_events_ratio,
    ROUND((UNIX_TIMESTAMP(first_session_start) - UNIX_TIMESTAMP(session_end_month)) / 60, 1) AS minutes_session_end_month_to_first_session_start,
    TO_DATE(session_end_month) AS session_end_month_date
FROM per_group
ORDER BY first_session_start
