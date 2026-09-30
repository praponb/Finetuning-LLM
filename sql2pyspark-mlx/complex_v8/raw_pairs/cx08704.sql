WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM base_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND SIZE(t.tags) >= 2
              AND t.opened_at >= TIMESTAMP '2025-01-07 13:00:00'
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority, agent_id), 2) AS requester_id_vs_priority_agent_id_avg,
        requester_id - LAG(requester_id) OVER w_all_by_requester_id AS requester_id_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY requester_id), 3) AS requester_id_percent_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_priority_agent_id, 0), 4) AS requester_id_share_of_priority_agent_id
    FROM qualified
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id, id), w_priority_agent_id AS (PARTITION BY priority, agent_id)
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM with_windows s
),
derived AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(priority)) AS priority_len
    FROM with_counts
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    tags,
    requester_id_vs_priority_agent_id_avg,
    requester_id_change,
    requester_id_percent_rank,
    requester_id_share_of_priority_agent_id,
    first_ticket_closed_at,
    ticket_count,
    last_ticket_closed_at,
    priority_len,
    CASE WHEN priority IN ('P2', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
    UPPER(TRIM(priority)) AS priority_clean
FROM derived
ORDER BY first_ticket_closed_at DESC NULLS FIRST, requester_id_share_of_priority_agent_id DESC NULLS FIRST
