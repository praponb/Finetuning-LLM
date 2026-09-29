WITH covered AS (
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
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_type,
        priority,
        MIN(event_at) AS first_event_at,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        COUNT(*) AS n_rows
    FROM covered
    GROUP BY DATE_TRUNC('MONTH', event_at), event_type, priority
    HAVING SUM(hours_since_open) > 14.4
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(first_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_month), n_rows ORDER BY first_event_at DESC, event_at_month, event_type, priority ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_event_at,
        LAST_VALUE(n_rows) IGNORE NULLS OVER (ORDER BY first_event_at ASC, event_at_month, event_type, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_rows,
        ROUND(n_rows - AVG(n_rows) OVER w_n_rows_first_event_at_month, 2) AS n_rows_vs_n_rows_first_event_at_month_avg
    FROM grouped
    WINDOW w_n_rows_first_event_at_month AS (PARTITION BY n_rows, DATE_TRUNC('MONTH', first_event_at))
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
        CASE WHEN event_type IN ('replied', 'reopened') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM metrics
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_first_event_at, priority, avg_hours_since_open
    FROM annotated
) src
PIVOT (
    MAX(avg_hours_since_open)
    FOR priority IN ('P3', 'P2', 'P4', 'P1')
)
