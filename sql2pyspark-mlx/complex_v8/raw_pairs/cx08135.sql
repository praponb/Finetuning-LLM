WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        CASE WHEN event_type IN ('replied', 'escalated') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 09:00:00'
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
      AND s.event_at_label IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_event_at_week
    FROM without_matches
    WHERE TO_DATE(event_at_week) < DATE '2025-01-25'
)
-- Subtotals with CUBE
SELECT
    event_type_group,
    event_type,
    actor,
    MAX(minutes_event_at_to_event_at_week) AS max_minutes_event_at_to_event_at_week
FROM annotated
GROUP BY CUBE(event_type_group, event_type, actor)
ORDER BY event_type_group NULLS LAST, event_type NULLS LAST, actor NULLS LAST
