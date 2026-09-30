WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null,
        COUNT(*) FILTER (WHERE event_type IN ('replied', 'reopened', 'created')) AS n_event_type_in_replied_reopen
    FROM in_window
    WHERE hours_since_open >= 25.2
    GROUP BY ticket_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE n_event_type_is_not_null > (SELECT AVG(n_event_type_is_not_null) FROM per_group)
)
-- Final projection
SELECT
    ticket_id,
    n_event_type_is_not_null,
    n_event_type_in_replied_reopen,
    MONTH(first_event_at) AS first_event_at_month
FROM qualified
ORDER BY n_event_type_is_not_null DESC, ticket_id
LIMIT 5
