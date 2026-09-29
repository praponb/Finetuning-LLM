WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-02-06 10:00:00'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM scoped_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_count, priority, csat
    FROM with_stats
) src
PIVOT (
    SUM(csat) AS total, COUNT(csat) AS n
    FOR priority IN ('P1', 'P2', 'P4', 'P3')
)
ORDER BY ticket_count
