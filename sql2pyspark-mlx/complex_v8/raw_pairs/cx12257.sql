WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
    WHERE csat BETWEEN 1 AND 4
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        priority,
        burst_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
tagged AS (
    -- Label and score rows
    SELECT *, QUARTER(session_start) AS session_start_quarter
    FROM session_summary
),
grouped AS (
    -- Summarise per group
    SELECT
        burst_id,
        DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_ym,
        session_start_quarter,
        SUM(CASE WHEN priority IN ('P4', 'P1') THEN 1 ELSE 0 END) AS n_priority_in_p4_p1,
        SUM(session_requester_id) AS total_session_requester_id,
        MIN(session_requester_id) AS min_session_requester_id,
        ROUND(AVG(session_start_quarter), 2) AS avg_session_start_quarter,
        ROUND(STDDEV(duration_min), 2) AS stddev_duration_min,
        ROUND(STDDEV(session_requester_id), 2) AS stddev_session_requester_id
    FROM tagged
    GROUP BY burst_id, DATE_FORMAT(session_start, 'yyyy-MM'), session_start_quarter
    HAVING SUM(event_count) > 2
)
SELECT
    burst_id,
    session_start_quarter,
    n_priority_in_p4_p1,
    total_session_requester_id,
    min_session_requester_id,
    avg_session_start_quarter,
    stddev_duration_min,
    LENGTH(TRIM(session_start_ym)) AS session_start_ym_len
FROM grouped
ORDER BY session_start_quarter
