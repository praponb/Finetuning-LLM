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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        t.opened_at AS ticket_opened_at
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.agent_name,
        s.agent_hired_on,
        s.agent_team,
        s.ticket_opened_at,
        su.team AS agent_team_2,
        su.tier AS agent_tier,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags
    FROM combined_details s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    agent_name,
    agent_hired_on,
    agent_team,
    ticket_opened_at,
    agent_team_2,
    agent_tier,
    ticket_channel,
    ticket_tags
FROM with_details
