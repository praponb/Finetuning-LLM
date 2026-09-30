WITH joined_ranges AS (
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
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY hours_since_open) AS hours_since_open_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        MAX(hours_since_open) OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hours_since_open_so_far,
        NTH_VALUE(event_at, 2) OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(hours_since_open, 2) OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hours_since_open
    FROM joined_ranges
)
-- Subtotals with CUBE
SELECT
    agent_id,
    hours_since_open_dense_rank,
    event_type,
    GROUPING(agent_id) AS is_total_agent_id,
    MAX(event_at) AS last_event_at
FROM sequenced
GROUP BY CUBE(agent_id, hours_since_open_dense_rank, event_type)
