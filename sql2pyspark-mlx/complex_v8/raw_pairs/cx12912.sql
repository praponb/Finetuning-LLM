WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_session_start_ties AS session_start_dense_rank,
        ROW_NUMBER() OVER (ORDER BY session_start DESC, agent_id, session_no) AS seq_no,
        FIRST_VALUE(session_end) IGNORE NULLS OVER (ORDER BY session_start DESC, agent_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_end,
        NTILE(5) OVER (ORDER BY session_start DESC, agent_id, session_no) AS session_start_quintile,
        ROUND(AVG(duration_min) OVER (ORDER BY session_start DESC, agent_id, session_no ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_3
    FROM sessions
    WINDOW w_all_by_session_start_ties AS (ORDER BY session_start DESC), w_all_by_session_start AS (ORDER BY session_start DESC, agent_id, session_no)
)
SELECT
    agent_id,
    session_no,
    session_start,
    session_end,
    events,
    duration_min,
    session_start_dense_rank,
    first_known_session_end,
    session_start_quintile
FROM with_windows
WHERE session_end >= TIMESTAMP '2025-01-13 09:00:00'
ORDER BY session_end DESC
