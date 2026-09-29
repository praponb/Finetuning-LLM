WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour_2
    FROM ticket_events
    WHERE actor = 'system'
      AND HOUR(event_at) BETWEEN 9 AND 11
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_hour_2,
        LAG(event_at) OVER (ORDER BY event_at_hour DESC, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at_hour) - UNIX_TIMESTAMP(LAG(event_at_hour) OVER w_all_by_event_at_hour)) / 60, 1) AS minutes_since_prev_event_at_hour,
        LAST_VALUE(event_at) OVER (ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at_hour_2) OVER (ORDER BY event_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_hour_2,
        DENSE_RANK() OVER (ORDER BY event_at_hour DESC) AS event_at_hour_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_hour DESC), 3) AS event_at_hour_cume_dist
    FROM prepared_events
    WINDOW w_all_by_event_at_hour AS (ORDER BY event_at_hour DESC, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE event_at_hour_dense_rank <= 2
      AND minutes_since_prev_event_at_hour < 3750.0
)
-- Subtotals with ROLLUP
SELECT
    actor,
    event_type,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(SUM(minutes_since_prev_event_at_hour) / NULLIF(SUM(event_at_hour_cume_dist), 0), 3) AS minutes_since_prev_event_at_hour_to_event_at_hou,
    MAX(event_at_hour_cume_dist) AS max_event_at_hour_cume_dist
FROM first_rows
GROUP BY ROLLUP(actor, event_type)
ORDER BY actor NULLS LAST, event_type NULLS LAST
