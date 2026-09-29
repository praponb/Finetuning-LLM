WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT *
FROM (
    SELECT event_at_month, ticket_id, actor, is_new_session
    FROM sessionized
) src
PIVOT (
    AVG(is_new_session)
    FOR actor IN ('customer', 'system')
)
ORDER BY event_at_month, ticket_id
