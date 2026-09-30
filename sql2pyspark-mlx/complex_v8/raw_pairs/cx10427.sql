WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE (actor <> 'agent' OR MONTH(event_at) IN (2, 3))
),
with_details AS (
    -- Join tickets
    SELECT s.*, t.opened_at AS ticket_opened_at, t.tags AS ticket_tags
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_hour,
        event_at_label,
        ticket_opened_at,
        ticket_tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at_hour DESC, id DESC) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
),
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(event_at_hour, 0) AS event_at_hour_filled
    FROM unique_rows
),
derived AS (
    -- Add calculated columns
    SELECT *, YEAR(ticket_opened_at) AS ticket_opened_at_year
    FROM tagged
    WHERE event_at_hour < 12
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_at_hour,
        s.ticket_opened_at,
        s.ticket_opened_at_year,
        t.requester_id AS ticket_requester_id
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id BETWEEN 6 AND 17
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    actor,
    ticket_opened_at,
    ticket_opened_at_year,
    ticket_csat,
    ticket_closed_at,
    agent_hired_on,
    ABS(ticket_csat - 3) AS ticket_csat_distance,
    CONCAT_WS('|', actor, agent_tier) AS actor_agent_tier_label
FROM joined_ticket
ORDER BY actor DESC, actor_agent_tier_label DESC
