WITH in_window AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY hours_since_open), 3) AS hours_since_open_percent_rank,
        SUM(hours_since_open) OVER (PARTITION BY event_type, ticket_id) AS event_type_ticket_id_sum_hours_since_open,
        LAG(hours_since_open, 3) OVER (ORDER BY hours_since_open, id) AS prev3_hours_since_open,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        hours_since_open - LAG(hours_since_open) OVER (ORDER BY hours_since_open, id) AS hours_since_open_change,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY event_type, ticket_id)) AS distinct_agent_id_in_event_type_ticket_id
    FROM in_window
)
SELECT
    id,
    event_type,
    agent_id,
    hours_since_open,
    event_type_ticket_id_sum_hours_since_open,
    prev3_hours_since_open,
    first_known_event_at,
    distinct_agent_id_in_event_type_ticket_id,
    DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
    ROUND(prev3_hours_since_open * 1.1, 2) AS prev3_hours_since_open_adj
FROM ranked
ORDER BY days_since_event_at DESC NULLS FIRST
