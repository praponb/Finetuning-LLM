WITH joined_ranges AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.agent_name,
        su.tier AS agent_tier
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.tier AS agent_tier_2,
        su.agent_name AS agent_agent_name,
        su.hired_on AS agent_hired_on
    FROM combined_details_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team IS NOT NULL
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.agent_name,
        s.agent_tier_2,
        s.agent_agent_name,
        s.agent_hired_on,
        t.id AS ticket_id_2,
        t.opened_at AS ticket_opened_at,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM combined_details s
    JOIN tickets t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    agent_id,
    agent_name,
    agent_tier_2,
    agent_agent_name,
    agent_hired_on,
    ticket_id_2,
    ticket_opened_at,
    ticket_priority,
    ticket_csat
FROM detail_rows_tickets
WHERE UPPER(TRIM(agent_agent_name)) LIKE 'G%'
ORDER BY ticket_id_2 DESC, id DESC
