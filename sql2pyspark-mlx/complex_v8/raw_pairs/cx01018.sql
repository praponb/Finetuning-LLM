WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-12 11:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at_year)) / 60, 1) AS minutes_closed_at_year_to_opened_at,
        MONTH(opened_at) AS opened_at_month
    FROM filtered
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.minutes_closed_at_year_to_opened_at,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM tagged s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.minutes_closed_at_year_to_opened_at,
        s.event_id,
        s.event_type,
        t.id AS event_id_2,
        t.event_at AS event_event_at,
        t.actor AS event_actor_2
    FROM detail_rows s
    JOIN ticket_events t ON t.ticket_id = s.id AND HOUR(t.event_at) BETWEEN 8 AND 13
)
-- Subtotals with CUBE
SELECT
    COALESCE(event_actor_2, 'ALL') AS event_actor_2,
    event_id,
    COUNT(*) AS row_count
FROM with_children_events
GROUP BY CUBE(event_actor_2, event_id)
