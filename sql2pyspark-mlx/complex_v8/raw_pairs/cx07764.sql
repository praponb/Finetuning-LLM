WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type = 'created'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM filtered_events
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(first_event_at) OVER w_first_event_at_year_event_ty AS next_first_event_at,
        LAST_VALUE(first_event_at) OVER (PARTITION BY YEAR(first_event_at), event_type ORDER BY first_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_event_at,
        DENSE_RANK() OVER (PARTITION BY YEAR(first_event_at), event_type ORDER BY first_event_at DESC, id) AS first_event_at_dense_rank
    FROM with_windows
    WINDOW w_first_event_at_year_event_ty AS (PARTITION BY YEAR(first_event_at), event_type ORDER BY first_event_at DESC, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(event_type) OVER w_first_event_at_dense_rank) AS distinct_event_type_in_first_event_at_dense_rank,
        LAST_VALUE(last_first_event_at) OVER (PARTITION BY event_type, YEAR(first_known_event_at) ORDER BY first_known_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_first_event_at,
        DENSE_RANK() OVER w_event_type_first_known_event_2 AS first_known_event_at_dense_rank,
        COUNT(*) OVER w_first_event_at_dense_rank AS rows_in_first_event_at_dense_rank,
        MIN(first_event_at_dense_rank) OVER (PARTITION BY first_event_at_dense_rank) AS first_event_at_dense_rank_min_first_event_at_den
    FROM sequenced
    WINDOW w_first_event_at_dense_rank AS (PARTITION BY first_event_at_dense_rank), w_event_type_first_known_event_2 AS (PARTITION BY event_type, YEAR(first_known_event_at) ORDER BY first_known_event_at)
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(first_event_at_dense_rank) OVER (PARTITION BY first_known_event_at_dense_rank, YEAR(last_last_first_event_at) ORDER BY first_known_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_first_event_at_dense_rank_so_far,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY distinct_event_type_in_first_event_at_dense_rank)) AS distinct_event_type_in_distinct_event_type_in_fi
    FROM windowed
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(first_known_event_at) AS first_known_event_at_day,
        TO_DATE(next_first_event_at) AS next_first_event_at_day,
        event_type,
        SUM(CASE WHEN event_type IN ('resolved', 'escalated', 'replied') THEN 1 ELSE 0 END) AS n_event_type_in_resolved_escal,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS n_rows,
        ROUND(AVG(first_known_event_at_dense_rank), 2) AS avg_first_known_event_at_dense_rank,
        MAX(last_first_event_at) AS last_last_first_event_at_2
    FROM windowed_2
    WHERE distinct_event_type_in_distinct_event_type_in_fi < 8
    GROUP BY TO_DATE(first_known_event_at), TO_DATE(next_first_event_at), event_type
    HAVING SUM(max_first_event_at_dense_rank_so_far) > 31
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE n_event_type_in_resolved_escal > (SELECT AVG(n_event_type_in_resolved_escal) FROM summary)
)
-- Final projection
SELECT
    first_known_event_at_day,
    next_first_event_at_day,
    event_type,
    n_event_type_in_resolved_escal,
    event_type_list,
    avg_first_known_event_at_dense_rank,
    last_last_first_event_at_2
FROM without_matches
