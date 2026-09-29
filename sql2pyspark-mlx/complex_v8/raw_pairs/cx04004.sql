WITH covered AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        t.id AS ticket_id_2,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id
    FROM covered s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel = 'email'
              AND MONTH(t.opened_at) IN (1, 3)
        )
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.ticket_id_2,
        s.ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM with_activity s
    LEFT JOIN tickets t ON t.id = s.ticket_id_2
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    ticket_id,
    agent_id,
    ticket_id_2,
    ticket_opened_at,
    ticket_agent_id,
    agent_hired_on,
    agent_team,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
FROM joined_ticket
WHERE ticket_id_2 IS NOT NULL
ORDER BY ticket_agent_id DESC, id, ticket_id_2
LIMIT 10
