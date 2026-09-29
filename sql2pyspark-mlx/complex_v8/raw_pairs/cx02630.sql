WITH matched AS (
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
latest_per_key AS (
    -- Keep the latest row per ticket_id, agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, agent_id ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM matched
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open
FROM latest_per_key
ORDER BY agent_id DESC, priority DESC
