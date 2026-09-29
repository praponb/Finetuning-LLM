WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
),
with_parent_agent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier, su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        closed_at_label,
        channel_priority_label,
        agent_tier,
        agent_hired_on,
        LAST_VALUE(closed_at) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, channel), 0), 4) AS requester_id_share_of_agent_id_channel
    FROM with_parent_agent
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at < TIMESTAMP '2025-02-27 06:00:00'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    MAX(agent_hired_on) AS last_agent_hired_on,
    MAX(requester_id) AS max_requester_id,
    COUNT(*) AS n_rows
FROM screened
GROUP BY GROUPING SETS ((priority, agent_tier), (priority), ())
ORDER BY priority NULLS LAST, agent_tier NULLS LAST
