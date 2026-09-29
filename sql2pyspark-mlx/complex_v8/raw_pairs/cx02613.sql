WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        QUARTER(event_at) AS event_at_quarter,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_at_quarter,
        event_at_label,
        SUM(event_at_quarter) OVER (ORDER BY event_at_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_quarter,
        SUM(event_at_quarter) OVER w_event_at_to_date AS event_at_to_date_sum_event_at_quarter,
        ROUND(event_at_quarter / NULLIF(SUM(event_at_quarter) OVER w_event_at_to_date, 0), 4) AS event_at_quarter_share_of_event_at_to_date
    FROM recent
    WINDOW w_event_at_to_date AS (PARTITION BY TO_DATE(event_at))
),
labeled AS (
    -- Add calculated columns
    SELECT *, ABS(event_at_quarter - 2) AS event_at_quarter_distance
    FROM windowed
    WHERE actor_event_type_label IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        running_event_at_quarter,
        event_at_to_date_sum_event_at_quarter,
        event_at_quarter_share_of_event_at_to_date,
        event_at_quarter_distance,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY running_event_at_quarter DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at_quarter_distance, 3) OVER (PARTITION BY actor ORDER BY running_event_at_quarter DESC, id) AS next3_event_at_quarter_distance,
        ROUND(event_at_quarter / NULLIF(SUM(event_at_quarter) OVER (PARTITION BY event_type, actor), 0), 4) AS event_at_quarter_share_of_event_type_actor
    FROM labeled
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    actor_event_type_label,
    running_event_at_quarter,
    event_at_quarter_distance,
    next3_event_at_quarter_distance
FROM with_trends
WHERE next3_event_at_quarter_distance <= 3
