WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at_2,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_type = 'reopened'
      AND ticket_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        days_since_event_at,
        event_type_actor_label,
        ticket_priority,
        ticket_tags,
        SUM(days_since_event_at) OVER (PARTITION BY ticket_priority, event_type ORDER BY days_since_event_at_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_event_at,
        RANK() OVER (PARTITION BY ticket_priority, event_type ORDER BY days_since_event_at_2, id) AS days_since_event_at_2_rank,
        MIN(ticket_requester_id) OVER (PARTITION BY ticket_priority, event_type) AS ticket_priority_event_type_min_ticket_requester_,
        SUM(ticket_requester_id) OVER (PARTITION BY ticket_priority, event_type ORDER BY days_since_event_at_2, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_sum_6_prior,
        SIZE(COLLECT_SET(ticket_id) OVER w_ticket_priority_event_type) AS distinct_ticket_id_in_ticket_priority_event_type
    FROM with_details
    WINDOW w_ticket_priority_event_type AS (PARTITION BY ticket_priority, event_type)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_ticket_id_in_ticket_priority_event_type,
    event_type,
    MIN(event_at) AS first_event_at,
    SUM(CASE WHEN ticket_priority = 'P4' THEN 1 ELSE 0 END) AS n_ticket_priority_p4,
    MAX(event_at) AS last_event_at
FROM metrics
GROUP BY GROUPING SETS ((distinct_ticket_id_in_ticket_priority_event_type, event_type), (distinct_ticket_id_in_ticket_priority_event_type))
ORDER BY distinct_ticket_id_in_ticket_priority_event_type NULLS LAST, event_type NULLS LAST
