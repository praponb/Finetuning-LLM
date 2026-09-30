WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE requester_id > 11
      AND agent_id IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT *, HOUR(closed_at) AS closed_at_hour, MONTH(closed_at) AS closed_at_month
    FROM cleaned_tickets
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-02-06 10:00:00'
        )
      AND ARRAY_CONTAINS(s.tags, 'login')
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.closed_at_hour,
        s.closed_at_month,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM with_activity s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    closed_at_month,
    closed_at_hour,
    GROUPING_ID() AS grouping_level,
    MAX(closed_at_hour) AS max_closed_at_hour
FROM detail_rows
GROUP BY GROUPING SETS ((event_type, closed_at_month, closed_at_hour), (event_type), ())
ORDER BY event_type NULLS LAST, closed_at_month NULLS LAST, closed_at_hour NULLS LAST
