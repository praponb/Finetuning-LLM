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
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN hours_since_open >= 57.6 THEN 'A' WHEN hours_since_open >= 25.2 THEN 'B' ELSE 'C' END AS hours_since_open_tier
    FROM matched
    WHERE ticket_id IS NOT NULL
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        hours_since_open_tier,
        ticket_id,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open,
        COUNT(*) AS n_rows
    FROM tagged
    GROUP BY DATE_TRUNC('MONTH', event_at), hours_since_open_tier, ticket_id
    HAVING COUNT(*) >= 2
)
SELECT
    event_at_month,
    hours_since_open_tier,
    ticket_id,
    median_hours_since_open,
    n_rows,
    ROUND(n_rows / NULLIF(median_hours_since_open, 0), 3) AS n_rows_per_median_hours_since_open
FROM per_group
ORDER BY ticket_id, median_hours_since_open, event_at_month, hours_since_open_tier
LIMIT 20
