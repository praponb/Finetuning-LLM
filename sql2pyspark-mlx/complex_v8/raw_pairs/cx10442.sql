WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        CASE WHEN requester_id >= 17 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier
    FROM tickets
    WHERE requester_id >= 7
),
with_prev AS (
    -- Previous closed_at per requester_id_tier
    SELECT *, LAG(closed_at) OVER (PARTITION BY requester_id_tier ORDER BY closed_at, id) AS prev_closed_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY requester_id_tier ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        requester_id_tier,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY requester_id_tier, session_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        session_end + INTERVAL 2 HOURS AS session_end_plus_2h,
        DAYOFWEEK(session_end) AS session_end_dayofweek,
        n_events + duration_min AS n_events_plus_duration_min
    FROM per_session
)
-- Final projection
SELECT
    requester_id_tier,
    session_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    n_events_plus_duration_min,
    TO_DATE(session_end_plus_2h) AS session_end_plus_2h_date
FROM labeled
