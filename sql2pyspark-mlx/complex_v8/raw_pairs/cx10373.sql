WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE actor = 'system'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at), 3) AS event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_event_type_event_at_month_by, 3) AS event_at_percent_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type ORDER BY event_at), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at) AS event_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, MONTH(event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_3,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_4
    FROM valid
    WINDOW w_event_type_event_at_month_by AS (PARTITION BY event_type, MONTH(event_at) ORDER BY event_at ASC)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        actor,
        event_at_label,
        next_event_at,
        minutes_since_prev_event_at,
        second_event_at_2,
        event_at_percent_rank,
        event_at_percent_rank_2,
        event_at_cume_dist,
        first_known_event_at,
        first_event_at,
        event_at_dense_rank,
        first_known_event_at_3,
        first_known_event_at_4,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM with_trends
)
SELECT
    id,
    actor,
    event_at_label,
    second_event_at_2,
    event_at_percent_rank,
    event_at_percent_rank_2,
    event_at_cume_dist,
    first_known_event_at_3,
    days_since_event_at
FROM labeled
ORDER BY first_known_event_at_3, id
LIMIT 5
