WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
    FROM tickets
    WHERE csat BETWEEN 2 AND 4
      AND MONTH(closed_at) IN (1, 2)
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM scoped_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND TO_DATE(t.closed_at) < DATE '2025-03-14'
        )
      AND s.closed_at_label IS NOT NULL
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.closed_at_label,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_actor, 'ALL') AS event_actor,
    COALESCE(priority, 'ALL') AS priority,
    COUNT(*) AS cnt,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM line_level_events
GROUP BY GROUPING SETS ((event_actor, priority), (event_actor), (priority), ())
ORDER BY event_actor NULLS LAST, priority NULLS LAST
