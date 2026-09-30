WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, YEAR(event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, YEAR(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id), 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at ASC NULLS LAST), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY MONTH(event_at), event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY event_type, YEAR(event_at) ORDER BY event_at DESC, id) AS next2_event_at
    FROM cleaned
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_label,
        minutes_since_prev_event_at_2,
        second_event_at,
        event_at_cume_dist,
        next2_event_at,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER (PARTITION BY ticket_id, event_type), 0), 4) AS event_at_cume_dist_share_of_ticket_id_event_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at_cume_dist, id), 3) AS event_at_cume_dist_percent_rank,
        ROUND((UNIX_TIMESTAMP(first_known_event_at) - UNIX_TIMESTAMP(LAG(first_known_event_at) OVER (PARTITION BY event_type ORDER BY event_at_cume_dist, id))) / 60, 1) AS minutes_since_prev_first_known_event_at
    FROM with_trends
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(next2_event_at) AS next2_event_at_year,
        actor,
        event_type,
        ROUND(SUM(event_at_cume_dist) / NULLIF(SUM(minutes_since_prev_event_at_2), 0), 3) AS event_at_cume_dist_to_minutes_since_prev_event_a,
        PERCENTILE_APPROX(event_at_cume_dist, 0.5) AS median_event_at_cume_dist,
        MAX(next2_event_at) AS last_next2_event_at,
        ROUND(AVG(minutes_since_prev_first_known_event_at), 2) AS avg_minutes_since_prev_first_known_event_at,
        COUNT(*) AS row_count
    FROM with_windows
    WHERE event_at_cume_dist < 0.8
    GROUP BY 1, 2, 3
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        next2_event_at_year,
        actor,
        event_type,
        event_at_cume_dist_to_minutes_since_prev_event_a,
        median_event_at_cume_dist,
        last_next2_event_at,
        avg_minutes_since_prev_first_known_event_at,
        SUM(median_event_at_cume_dist) OVER (PARTITION BY row_count, event_type) AS row_count_event_type_sum_median_event_at_cume_di,
        ROUND((UNIX_TIMESTAMP(last_next2_event_at) - UNIX_TIMESTAMP(LAG(last_next2_event_at) OVER (PARTITION BY row_count ORDER BY median_event_at_cume_dist ASC, next2_event_at_year, actor, event_type))) / 60, 1) AS minutes_since_prev_last_next2_event_at,
        LEAD(last_next2_event_at) OVER w_row_count_by_median_event_at AS next_last_next2_event_at
    FROM totals
    WINDOW w_row_count_by_median_event_at AS (PARTITION BY row_count ORDER BY median_event_at_cume_dist ASC, next2_event_at_year, actor, event_type)
)
-- Final projection
SELECT
    next2_event_at_year,
    actor,
    event_type,
    median_event_at_cume_dist,
    last_next2_event_at,
    avg_minutes_since_prev_first_known_event_at,
    row_count_event_type_sum_median_event_at_cume_di,
    minutes_since_prev_last_next2_event_at,
    next_last_next2_event_at
FROM with_trends_2
WHERE event_at_cume_dist_to_minutes_since_prev_event_a <= 8.0
ORDER BY actor DESC NULLS FIRST, avg_minutes_since_prev_first_known_event_at
