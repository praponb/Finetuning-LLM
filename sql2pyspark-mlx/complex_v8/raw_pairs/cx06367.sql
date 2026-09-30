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
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY agent_id, ticket_id ORDER BY event_at, id) AS next_event_at,
        ROUND(CUME_DIST() OVER w_event_at_month_priority_by_h, 3) AS hours_since_open_cume_dist,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), priority ORDER BY hours_since_open, id) AS hours_since_open_quartile
    FROM in_window
    WINDOW w_event_at_month_priority_by_h AS (PARTITION BY DATE_TRUNC('MONTH', event_at), priority ORDER BY hours_since_open), w_agent_id AS (PARTITION BY agent_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type,
        SUM(hours_since_open_cume_dist) FILTER (WHERE event_type IN ('escalated', 'created')) AS hours_since_open_cume_dist_event_type_in_escalat,
        SUM(hours_since_open_quartile) AS total_hours_since_open_quartile
    FROM windowed
    GROUP BY 1
    HAVING SUM(hours_since_open_quartile) > 2
)
SELECT
    event_type,
    hours_since_open_cume_dist_event_type_in_escalat,
    total_hours_since_open_quartile,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    INITCAP(TRIM(event_type)) AS event_type_clean
FROM per_group
WHERE hours_since_open_cume_dist_event_type_in_escalat >= 2.2
ORDER BY event_type_clean
