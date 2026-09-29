WITH matched AS (
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
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IS NOT NULL) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM matched s
),
unique_rows AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        event_event_type_count,
        event_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    event_at,
    priority,
    agent_id,
    event_event_type_count,
    event_count,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM unique_rows
ORDER BY event_type_len DESC
