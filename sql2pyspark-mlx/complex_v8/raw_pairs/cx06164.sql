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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.tier AS agent_tier,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM covered s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        t.closed_at AS ticket_closed_at_2,
        t.tags AS ticket_tags
    FROM combined_details s
    JOIN support_agents su ON su.id = s.agent_id AND LENGTH(TRIM(su.agent_name)) >= 4
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT ticket_closed_at_2, ticket_closed_at, event_type, hours_since_open
    FROM joined_agent
) src
PIVOT (
    AVG(hours_since_open) AS avg_v, MAX(hours_since_open) AS max_v
    FOR event_type IN ('escalated', 'resolved', 'replied', 'assigned')
)
ORDER BY ticket_closed_at_2, ticket_closed_at
