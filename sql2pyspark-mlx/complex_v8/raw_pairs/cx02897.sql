WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE priority NOT IN ('P2', 'P4')
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        priority,
        session_id,
        session_start,
        session_end,
        events,
        duration_min,
        NTILE(3) OVER (PARTITION BY session_id ORDER BY session_csat NULLS LAST, priority, session_id) AS session_csat_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY session_id ORDER BY session_csat NULLS LAST, priority, session_id), 3) AS session_csat_cume_dist,
        ROUND(AVG(session_csat) OVER (PARTITION BY session_id ORDER BY session_csat NULLS LAST, priority, session_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS session_csat_avg_3,
        LAST_VALUE(duration_min) OVER (PARTITION BY session_id ORDER BY session_csat NULLS LAST, priority, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_min,
        NTH_VALUE(session_id, 2) OVER (PARTITION BY session_id ORDER BY session_csat NULLS LAST, priority, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_id
    FROM sessions
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_csat_tercile, priority, session_csat_avg_3
    FROM sequenced
) src
PIVOT (
    AVG(session_csat_avg_3)
    FOR priority IN ('P4', 'P2', 'P3', 'P1')
)
ORDER BY session_csat_tercile
