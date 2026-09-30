WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'system'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at, 2) OVER (ORDER BY event_at, id) AS prev2_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        LEAD(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at, id) AS next3_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at, id), 3) AS event_at_percent_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LAG(event_at) OVER w_event_type_ticket_id_by_even AS prev_event_at_2,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at) AS event_at_rank,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS rn
    FROM scoped_events
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(event_at_cume_dist - 0.5) AS event_at_cume_dist_distance,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM with_trends
    WHERE MONTH(first_known_event_at) IN (2, 3)
)
SELECT
    id,
    ticket_id,
    event_at,
    prev_event_at,
    first_event_at,
    prev2_event_at,
    first_known_event_at,
    first_event_at_2,
    next3_event_at,
    event_at_percent_rank,
    first_event_at_3,
    event_at_percent_rank_2,
    event_at_cume_dist,
    prev_event_at_2,
    event_at_rank,
    event_at_cume_dist_distance,
    event_type_clean
FROM enriched
ORDER BY prev_event_at_2 DESC, id DESC NULLS FIRST
