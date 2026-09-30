WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
    WHERE channel NOT IN ('chat', 'phone')
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND SIZE(t.tags) >= 1
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(priority_or_unknown, 'ALL') AS priority_or_unknown,
    agent_id,
    COALESCE(priority, 'ALL') AS priority,
    MIN(csat) AS min_csat,
    COUNT(*) AS cnt
FROM with_activity
GROUP BY GROUPING SETS ((priority_or_unknown, agent_id, priority), (priority_or_unknown), (agent_id), ())
