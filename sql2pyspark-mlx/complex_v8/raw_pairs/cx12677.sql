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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM joined_ranges s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM with_activity s
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(event_count) IGNORE NULLS OVER (PARTITION BY priority ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_count,
        LAST_VALUE(event_count) IGNORE NULLS OVER (PARTITION BY event_count ORDER BY event_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_count_2
    FROM summarized_rows
)
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    first_event_at,
    event_count,
    last_known_event_count,
    last_known_event_count_2
FROM metrics
ORDER BY agent_id NULLS LAST, id
LIMIT 5
