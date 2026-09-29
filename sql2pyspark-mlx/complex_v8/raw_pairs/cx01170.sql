WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE (event_type NOT IN ('created', 'escalated') OR TO_DATE(event_at) < DATE '2025-02-06')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_slug,
        event_at_week,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER (PARTITION BY event_type ORDER BY event_at_week, id))) / 60, 1) AS minutes_since_prev_event_at_week,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at_week, id), 3) AS event_at_week_percent_rank,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at_week) AS event_at_week_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY event_at_week, id) AS event_at_week_decile
    FROM base_events
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE event_at_week_dense_rank = 1
      AND second_event_at < TIMESTAMP '2025-02-12 13:00:00'
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM top_per_group s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
)
SELECT *
FROM (
    SELECT event_at_week_decile, event_type, event_at_week_percent_rank
    FROM screened
) src
PIVOT (
    AVG(event_at_week_percent_rank)
    FOR event_type IN ('assigned', 'created', 'reopened', 'resolved')
)
