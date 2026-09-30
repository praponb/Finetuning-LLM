WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE priority = 'P3'
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        burst_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
totals AS (
    -- Summarise per group
    SELECT
        events,
        DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_ym,
        ROUND(AVG(events), 2) AS avg_events,
        MIN(duration_min) AS min_duration_min,
        SUM(burst_id) FILTER (WHERE priority IN ('P4', 'P3', 'P2')) AS burst_id_priority_in_p4_p3_p2,
        ROUND(STDDEV(burst_id), 2) AS stddev_burst_id,
        SUM(burst_id) AS total_burst_id,
        SUM(duration_min) AS total_duration_min
    FROM per_session
    GROUP BY events, DATE_FORMAT(session_start, 'yyyy-MM')
    HAVING SUM(duration_min) > 84.0
)
SELECT
    events,
    session_start_ym,
    avg_events,
    min_duration_min,
    burst_id_priority_in_p4_p3_p2,
    stddev_burst_id,
    total_duration_min,
    LOWER(TRIM(session_start_ym)) AS session_start_ym_clean,
    CASE WHEN min_duration_min >= 192.0 THEN 'HIGH' WHEN min_duration_min >= 72.0 THEN 'MEDIUM' ELSE 'LOW' END AS min_duration_min_tier
FROM totals
ORDER BY burst_id_priority_in_p4_p3_p2, stddev_burst_id, events, session_start_ym
LIMIT 10
