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
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open - LAG(hours_since_open) OVER (PARTITION BY priority ORDER BY hours_since_open DESC NULLS FIRST, id) AS hours_since_open_change,
        MAX(hours_since_open) OVER (PARTITION BY event_type, ticket_id) AS event_type_ticket_id_max_hours_since_open,
        SUM(hours_since_open) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hours_since_open,
        DENSE_RANK() OVER w_ticket_id_agent_id_by_event AS event_at_dense_rank,
        SUM(hours_since_open) OVER (PARTITION BY event_type, ticket_id) AS event_type_ticket_id_sum_hours_since_open
    FROM in_window
    WINDOW w_ticket_id_agent_id_by_event AS (PARTITION BY ticket_id, agent_id ORDER BY event_at)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE event_at_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_ticket_id_max_hours_since_open,
    running_hours_since_open,
    event_at_dense_rank,
    MONTH(event_at) AS event_at_month,
    DATE_TRUNC('HOUR', event_at) AS event_at_hour
FROM top_per_group
ORDER BY event_type_ticket_id_max_hours_since_open, ticket_id NULLS LAST
