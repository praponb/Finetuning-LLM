WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type IN ('escalated', 'resolved')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        QUARTER(event_at) AS event_at_quarter,
        COALESCE(event_at_dayofweek, 0) AS event_at_dayofweek_filled
    FROM scoped_events
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority
    FROM labeled s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.ticket_priority,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at
    FROM with_parent_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    actor,
    ticket_priority,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
FROM detail_rows_events
GROUP BY GROUPING SETS ((event_type, actor, ticket_priority), (event_type), (actor), ())
