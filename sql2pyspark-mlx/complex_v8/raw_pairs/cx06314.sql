WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at
    FROM tickets
    WHERE (TO_DATE(opened_at) < DATE '2025-03-14' OR closed_at >= TIMESTAMP '2025-02-04 23:00:00')
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM filtered_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IS NOT NULL
        )
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.hours_closed_at_to_opened_at,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM without_matches s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-01-25 10:00:00'
)
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    hours_closed_at_to_opened_at,
    event_id,
    event_at,
    event_type
FROM line_level_events
ORDER BY agent_id, requester_id
