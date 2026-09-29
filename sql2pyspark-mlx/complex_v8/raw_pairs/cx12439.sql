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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        agent_id,
        agent_name,
        agent_hired_on,
        agent_tier,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY event_at), 3) AS event_at_percent_rank,
        NTILE(4) OVER (PARTITION BY agent_id, ticket_id ORDER BY agent_hired_on DESC, id) AS agent_hired_on_quartile,
        ROW_NUMBER() OVER w_agent_id_ticket_id_by_agent AS seq_no
    FROM with_details_agent
    WINDOW w_agent_id_ticket_id_by_agent AS (PARTITION BY agent_id, ticket_id ORDER BY agent_hired_on DESC, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    agent_id,
    agent_name,
    agent_hired_on,
    event_at_percent_rank,
    agent_hired_on_quartile,
    seq_no,
    TO_DATE(event_at) AS event_at_date,
    GREATEST(event_at_percent_rank, agent_hired_on_quartile) AS greatest_event_at_percent_rank_agent_hired_on_qu
FROM sequenced
ORDER BY agent_name NULLS LAST, event_at NULLS LAST
