WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id >= 9
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id < 18
              AND t.priority <> 'P4'
        )
      AND ARRAY_CONTAINS(s.tags, 'upgrade')
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    csat,
    tags,
    prev_closed_at,
    is_new_session,
    session_no,
    DATE_FORMAT(prev_closed_at, 'yyyy-MM') AS prev_closed_at_label,
    INITCAP(TRIM(priority)) AS priority_clean
FROM qualified
WHERE requester_id <= 17
ORDER BY priority DESC
