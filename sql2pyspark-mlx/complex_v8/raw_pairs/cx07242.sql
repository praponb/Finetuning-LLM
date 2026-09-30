WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE event_type = 'escalated'
),
labeled AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM scoped
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_prefix,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM labeled s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'agent'
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    ROUND(100 * COUNT(CASE WHEN event_actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_actor_is_not_null
FROM line_level
GROUP BY GROUPING SETS ((event_type, ticket_id), (event_type))
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST
