WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        TO_DATE(event_at) AS event_at_date,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        event_at_month,
        ROUND((UNIX_TIMESTAMP(event_at_month) - UNIX_TIMESTAMP(LAG(event_at_month) OVER w_event_type_by_event_at_month)) / 60, 1) AS minutes_since_prev_event_at_month,
        FIRST_VALUE(event_at_month) OVER (PARTITION BY event_type ORDER BY event_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at_month), 3) AS event_at_month_percent_rank,
        LAG(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at_month, id) AS prev2_event_at,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at_month) AS event_at_month_rank
    FROM prepared
    WINDOW w_event_type_by_event_at_month AS (PARTITION BY event_type ORDER BY event_at_month, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, TO_DATE(prev2_event_at) ORDER BY first_event_at_month), 3) AS first_event_at_month_percent_rank,
        ROUND(event_at_month_percent_rank / NULLIF(SUM(event_at_month_percent_rank) OVER (PARTITION BY MONTH(first_event_at_month), event_at_month_rank), 0), 4) AS event_at_month_percent_rank_share_of_first_event,
        COUNT(*) OVER (PARTITION BY event_type, TO_DATE(prev2_event_at) ORDER BY first_event_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        event_at_month_rank - LAG(event_at_month_rank) OVER (PARTITION BY event_type, TO_DATE(prev2_event_at) ORDER BY first_event_at_month, id) AS event_at_month_rank_change,
        COUNT(*) OVER (PARTITION BY MONTH(first_event_at_month), event_at_month_rank) AS rows_in_first_event_at_month_month_event_at_mont
    FROM windowed
    WINDOW w_event_type_prev2_event_at_to AS (PARTITION BY event_type, TO_DATE(prev2_event_at) ORDER BY first_event_at_month)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_at_month, event_type, event_at_month_rank_change
    FROM ranked
) src
PIVOT (
    COUNT(event_at_month_rank_change)
    FOR event_type IN ('resolved', 'assigned', 'escalated', 'replied')
)
ORDER BY event_at_month
