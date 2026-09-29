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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-19 11:00:00'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', event_type, priority) AS event_type_priority_label,
        CASE WHEN hours_since_open >= 50.4 THEN 'HIGH' WHEN hours_since_open >= 14.4 THEN 'MEDIUM' ELSE 'LOW' END AS hours_since_open_tier,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM qualified
),
event_type_stats AS (
    -- Median event_type_len per event_type
    SELECT event_type, PERCENTILE_APPROX(event_type_len, 0.5) AS median_event_type_len
    FROM tagged
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_type_len,
        ROUND(s.event_type_len - g.median_event_type_len, 2) AS event_type_len_minus_median
    FROM tagged s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_type_len >= g.median_event_type_len
)
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    agent_id,
    hours_since_open,
    event_type_priority_label,
    hours_since_open_tier,
    median_event_type_len
FROM vs_group
ORDER BY median_event_type_len NULLS LAST, hours_since_open
