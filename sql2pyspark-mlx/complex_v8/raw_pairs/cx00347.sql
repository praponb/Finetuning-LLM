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
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at
    FROM in_window s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.first_event_at,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        su.agent_name
    FROM with_stats s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = s.agent_id
    WHERE t.priority = 'P2'
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        s.first_event_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel
    FROM enriched s
    JOIN support_agents su ON su.id = s.agent_id
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    priority,
    agent_id,
    first_event_at,
    agent_team,
    agent_hired_on,
    agent_tier,
    ticket_tags,
    ticket_channel,
    UPPER(TRIM(agent_team)) AS agent_team_clean,
    LENGTH(TRIM(ticket_channel)) AS ticket_channel_len
FROM with_details_agent
ORDER BY id, agent_team
