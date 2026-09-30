WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type IN ('reopened', 'assigned', 'replied')
      AND actor IS NOT NULL
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details_ticket s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
      AND s.ticket_id <= 12
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.ticket_priority,
        t.csat AS ticket_csat,
        su.tier AS agent_tier
    FROM with_activity s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.csat < 5
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM combined_details s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_type,
    ticket_priority,
    ticket_csat,
    agent_tier,
    event_count,
    CASE WHEN event_count >= 6 THEN 'HIGH' WHEN event_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS event_count_tier,
    LEAST(event_count, ticket_csat) AS least_event_count_ticket_csat
FROM summarized_rows
WHERE event_type IN ('resolved', 'reopened')
ORDER BY ticket_id DESC
