WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d
    FROM ticket_events
    WHERE event_type IN ('reopened', 'resolved', 'assigned')
      AND MONTH(event_at) IN (1, 3)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at_plus_7d, id) AS event_at_plus_7d_dense_rank,
        NTH_VALUE(event_at_plus_7d, 2) OVER (ORDER BY event_at_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_7d,
        NTILE(4) OVER (ORDER BY event_at_plus_7d, id) AS event_at_plus_7d_quartile,
        NTILE(5) OVER (ORDER BY event_at_plus_7d, id) AS event_at_plus_7d_quintile,
        NTILE(3) OVER w_all_by_event_at_plus_7d AS event_at_plus_7d_tercile
    FROM cleaned_events
    WINDOW w_all_by_event_at_plus_7d AS (ORDER BY event_at_plus_7d, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(event_at_month) OVER (ORDER BY event_at_plus_7d_tercile NULLS LAST, id) AS next_event_at_month,
        MIN(event_at_plus_7d_quartile) OVER (PARTITION BY event_at_plus_7d_dense_rank) AS event_at_plus_7d_dense_rank_min_event_at_plus_7d,
        MAX(event_at_plus_7d_tercile) OVER (PARTITION BY event_at_plus_7d_dense_rank) AS event_at_plus_7d_dense_rank_max_event_at_plus_7d,
        RANK() OVER w_second_event_at_plus_7d_year AS second_event_at_plus_7d_rank,
        LAST_VALUE(event_at_hour) IGNORE NULLS OVER (ORDER BY event_at_plus_7d_dense_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_hour,
        ROUND(AVG(event_at_plus_7d_quintile) OVER (PARTITION BY event_at_plus_7d_dense_rank), 2) AS event_at_plus_7d_dense_rank_avg_event_at_plus_7d
    FROM metrics
    WINDOW w_second_event_at_plus_7d_year AS (PARTITION BY YEAR(second_event_at_plus_7d) ORDER BY second_event_at_plus_7d DESC, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type,
        event_at_hour,
        event_at_plus_7d,
        event_at_plus_7d_dense_rank,
        event_at_plus_7d_quintile,
        next_event_at_month,
        event_at_plus_7d_dense_rank_min_event_at_plus_7d,
        event_at_plus_7d_dense_rank_max_event_at_plus_7d,
        second_event_at_plus_7d_rank,
        last_known_event_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug_2
    FROM with_trends
)
SELECT
    id,
    event_type,
    event_at_hour,
    event_at_plus_7d_dense_rank,
    event_at_plus_7d_quintile,
    event_at_plus_7d_dense_rank_max_event_at_plus_7d,
    second_event_at_plus_7d_rank,
    event_type_actor_label,
    LENGTH(TRIM(event_type_slug)) AS event_type_slug_len,
    TO_DATE(next_event_at_month) AS next_event_at_month_date
FROM enriched
ORDER BY event_type_actor_label DESC, next_event_at_month_date DESC
