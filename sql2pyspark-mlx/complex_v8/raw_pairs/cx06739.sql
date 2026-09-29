WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.prev_closed_at,
        s.new_session_flag,
        s.burst_id,
        t.tag
    FROM sessionized s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    prev_closed_at,
    new_session_flag,
    burst_id,
    HOUR(closed_at) AS closed_at_hour
FROM per_label
ORDER BY agent_id, prev_closed_at DESC NULLS FIRST
