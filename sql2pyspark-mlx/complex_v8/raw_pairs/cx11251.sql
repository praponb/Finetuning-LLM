WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        ticket_opened_at,
        ticket_agent_id,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM combined_details_ticket
)
-- Subtotals with GROUPING SETS
SELECT
    actor_len,
    ticket_id,
    ticket_agent_id,
    ROUND(SUM(event_at_dayofweek) / NULLIF(SUM(actor_len), 0), 3) AS event_at_dayofweek_to_actor_len_ratio,
    MAX(ticket_opened_at) AS last_ticket_opened_at,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM derived
GROUP BY GROUPING SETS ((actor_len, ticket_id, ticket_agent_id), (actor_len), (ticket_id), ())
ORDER BY actor_len NULLS LAST, ticket_id NULLS LAST, ticket_agent_id NULLS LAST
