WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-27 00:00:00'
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
)
SELECT
    agent_id,
    burst_id,
    n_events,
    COUNT(DISTINCT agent_id) AS distinct_agent_ids,
    PERCENTILE_APPROX(n_events, 0.5) AS median_n_events,
    MIN(session_end) AS first_session_end
FROM sessions
GROUP BY GROUPING SETS ((agent_id, burst_id, n_events), (agent_id), (burst_id))
ORDER BY agent_id NULLS LAST, burst_id NULLS LAST, n_events NULLS LAST
