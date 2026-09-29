WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE ticket_id <= 19
      AND actor <> 'agent'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.ticket_agent_id,
        s.ticket_closed_at,
        s.ticket_priority,
        su.tier AS agent_tier,
        su.agent_name
    FROM with_details s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at
    FROM enriched
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        MAX(event_at) AS last_event_at,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(ticket_priority)) AS ticket_priority_list,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        SUM(CASE WHEN ticket_priority = 'P3' THEN 1 ELSE 0 END) AS n_ticket_priority_p3
    FROM with_windows
    GROUP BY event_type
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE n_ticket_priority_p3 > (SELECT MIN(n_ticket_priority_p3) FROM grouped)
)
-- Final projection
SELECT
    event_type,
    last_event_at,
    distinct_ids,
    ticket_priority_list,
    agent_name_list,
    ROUND(distinct_ids / NULLIF(n_ticket_priority_p3, 0), 3) AS distinct_ids_per_n_ticket_priority_p3,
    DATE_TRUNC('WEEK', last_event_at) AS last_event_at_week
FROM with_activity
ORDER BY last_event_at DESC
