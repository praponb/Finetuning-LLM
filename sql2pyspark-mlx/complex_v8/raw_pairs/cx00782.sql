WITH filtered AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
      AND event_type IN ('created', 'assigned', 'reopened')
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM detail_rows_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_id,
        s.event_actor,
        s.event_event_type,
        s.event_count,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at,
        ti.event_at AS event_event_at,
        ti.actor AS event_actor_2
    FROM summarized_rows s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN ticket_events ti ON ti.id = s.event_id
    WHERE t.agent_id IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_actor_2, 'ALL') AS event_actor_2,
    event_id,
    GROUPING_ID() AS grouping_level,
    SUM(event_count) FILTER (WHERE event_actor <> 'system') AS event_count_event_actor_system
FROM combined_details
GROUP BY GROUPING SETS ((event_actor_2, event_id), (event_actor_2))
