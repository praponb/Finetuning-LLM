WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type = 'replied'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_dayofweek,
        t.opened_at AS ticket_opened_at,
        su.agent_name
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id BETWEEN 5 AND 20
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'customer'
        )
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_dayofweek,
        s.agent_name,
        t.opened_at AS ticket_opened_at_2,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on
    FROM without_matches s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('WEEK', agent_hired_on) AS agent_hired_on_week
    FROM with_parent_ticket
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.ticket_opened_at_2,
        s.ticket_channel,
        s.agent_hired_on,
        s.agent_hired_on_week,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel_2,
        su.agent_name AS agent_agent_name
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    ticket_opened_at_2,
    ticket_channel,
    agent_hired_on,
    agent_hired_on_week,
    ticket_csat,
    ticket_tags,
    ticket_channel_2,
    agent_agent_name
FROM with_details
ORDER BY agent_hired_on_week DESC, id
LIMIT 20
