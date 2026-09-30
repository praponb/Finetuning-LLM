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
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        su.agent_name,
        su.tier AS agent_tier
    FROM in_window s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = s.agent_id
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        s.ticket_agent_id,
        s.ticket_requester_id,
        s.agent_name,
        s.agent_tier,
        t.id AS ticket_id_2,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id_2
    FROM enriched s
    JOIN tickets t ON t.agent_id = s.agent_id
)
SELECT
    id,
    ticket_id,
    ticket_requester_id,
    agent_tier,
    ticket_channel,
    LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug
FROM detail_rows_tickets
ORDER BY agent_tier, ticket_id NULLS LAST
