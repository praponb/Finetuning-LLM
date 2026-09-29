WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LOWER(TRIM(actor)) AS actor_clean,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id <= 38
      AND event_type = 'resolved'
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.opened_at < TIMESTAMP '2025-03-02 08:00:00'
              AND t.csat >= 2
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        actor_clean,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM filtered
    WHERE actor IN ('system', 'agent')
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_slug,
        s.event_type_or_unknown,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        su.agent_name
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id BETWEEN 2 AND 18
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_slug,
    ticket_tags,
    ticket_closed_at,
    agent_name
FROM joined_ticket
ORDER BY ticket_closed_at, agent_name DESC
