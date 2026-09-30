WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(duration_min / NULLIF(session_requester_id, 0), 3) AS duration_min_per_session_requester_id,
        DATE_TRUNC('HOUR', session_end) AS session_end_hour,
        ROUND(event_count / NULLIF(burst_id, 0), 3) AS event_count_per_burst_id
    FROM session_summary
    WHERE session_start >= TIMESTAMP '2025-01-25 09:00:00'
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    session_start,
    session_end,
    session_requester_id,
    duration_min,
    duration_min_per_session_requester_id,
    event_count_per_burst_id
FROM derived
WHERE event_count_per_burst_id >= 3.0
ORDER BY session_end DESC, agent_id, burst_id
LIMIT 5
