WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
stats AS (
    -- Group-level totals
    SELECT events, burst_id, SUM(burst_id) AS total_burst_id
    FROM session_summary
    GROUP BY events, burst_id
    HAVING AVG(events) > 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        events,
        burst_id,
        total_burst_id,
        COUNT(*) OVER (ORDER BY total_burst_id, events, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(total_burst_id, 3) OVER (ORDER BY total_burst_id, events, burst_id) AS prev3_total_burst_id
    FROM stats
)
SELECT
    events,
    burst_id,
    total_burst_id,
    running_count,
    prev3_total_burst_id
FROM sequenced
ORDER BY burst_id DESC, total_burst_id, events
LIMIT 10
