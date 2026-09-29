WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE channel IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        SUM(requester_id) AS total_ticket_requester_id,
        COUNT(*) FILTER (WHERE priority IN ('P2', 'P4', 'P1')) AS ticket_priority_count,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_requester_id,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        t.last_ticket_closed_at
    FROM filtered s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(closed_at) AS closed_at_year,
        DATEDIFF(DATE '2026-01-01', last_ticket_closed_at) AS days_since_last_ticket_closed_at
    FROM with_stats
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    ticket_priority_count,
    closed_at_year,
    GROUPING_ID() AS grouping_level,
    ROUND(100 * COUNT(CASE WHEN priority IN ('P1', 'P4', 'P3') THEN 1 END) / COUNT(*), 1) AS pct_priority_in_p1_p4_p3
FROM tagged
GROUP BY GROUPING SETS ((agent_id, ticket_priority_count, closed_at_year), (agent_id))
ORDER BY agent_id NULLS LAST, ticket_priority_count NULLS LAST, closed_at_year NULLS LAST
