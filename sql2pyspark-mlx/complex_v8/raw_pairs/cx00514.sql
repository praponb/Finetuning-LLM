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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
),
unique_rows AS (
    -- Keep the earliest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY hours_since_open, id) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    event_type,
    hours_since_open,
    CASE WHEN hours_since_open >= 43.2 THEN 'large' WHEN hours_since_open >= 18.0 THEN 'medium' ELSE 'small' END AS hours_since_open_tier,
    CONCAT(event_type, '#', CAST(agent_id AS STRING)) AS event_type_ref
FROM unique_rows
ORDER BY event_type_ref DESC, event_type DESC
