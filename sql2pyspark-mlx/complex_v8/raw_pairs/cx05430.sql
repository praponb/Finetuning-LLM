WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_no
)
SELECT *
FROM (
    SELECT session_no, priority, duration_min
    FROM sessions
) src
PIVOT (
    SUM(duration_min)
    FOR priority IN ('P2', 'P3')
)
