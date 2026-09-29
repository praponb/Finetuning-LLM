WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type <> 'replied'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_event_type_by_event_at_ties AS event_at_dense_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor)) AS distinct_ticket_id_in_actor,
        SUM(days_since_event_at) OVER (PARTITION BY actor) AS actor_sum_days_since_event_at
    FROM base_events
    WINDOW w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at)
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('replied', 'escalated', 'created')
              AND TO_DATE(t.event_at) < DATE '2025-03-08'
        )
      AND s.ticket_id >= 4
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_event_at,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND TO_DATE(t.event_at) < DATE '2025-03-08'
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.days_since_event_at,
        s.event_id,
        s.event_event_at,
        s.event_event_type,
        t.priority AS ticket_priority,
        su.tier AS agent_tier
    FROM exploded_detail_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_priority,
    event_event_type,
    GROUPING(ticket_priority) AS is_total_ticket_priority,
    COUNT(*) AS cnt,
    MAX(days_since_event_at) AS max_days_since_event_at,
    ROUND(AVG(days_since_event_at), 2) AS avg_days_since_event_at
FROM enriched_ticket
GROUP BY GROUPING SETS ((ticket_priority, event_event_type), (ticket_priority), ())
ORDER BY ticket_priority NULLS LAST, event_event_type NULLS LAST
