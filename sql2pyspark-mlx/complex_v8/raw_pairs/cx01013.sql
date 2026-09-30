WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id <= 53
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', prev_event_at) AS prev_event_at_month,
        ticket_id,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        SUM(days_since_event_at) AS total_days_since_event_at
    FROM sessionized
    GROUP BY DATE_TRUNC('MONTH', prev_event_at), ticket_id
)
-- Final projection
SELECT
    prev_event_at_month,
    ticket_id,
    cnt,
    total_days_since_event_at,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    TO_DATE(prev_event_at_month) AS prev_event_at_month_date
FROM summary
ORDER BY cnt_bucket DESC NULLS FIRST, ticket_id DESC NULLS FIRST, prev_event_at_month
LIMIT 5
