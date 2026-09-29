WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at < TIMESTAMP '2025-02-18 09:00:00'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        actor,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(3) OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_tercile
    FROM scoped_events
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(event_at_tercile / 2) * 2 AS event_at_tercile_bucket,
        FLOOR(event_at_tercile / 2) * 2 AS event_at_tercile_bucket_2
    FROM with_windows
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        second_event_at,
        COUNT(*) OVER (PARTITION BY event_type, event_at_tercile) AS rows_in_event_type_event_at_tercile,
        ROUND(event_at_tercile_bucket_2 - AVG(event_at_tercile_bucket_2) OVER w_event_type_event_at_tercile, 2) AS event_at_tercile_bucket_2_vs_event_type_event_at
    FROM annotated
    WINDOW w_event_type_event_at_tercile AS (PARTITION BY event_type, event_at_tercile)
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(second_event_at) AS second_event_at_day,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MAX(second_event_at) AS last_second_event_at,
        MAX(rows_in_event_type_event_at_tercile) AS max_rows_in_event_type_event_at_tercile
    FROM ranked
    GROUP BY TO_DATE(second_event_at)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        second_event_at_day,
        distinct_event_types,
        last_second_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_max_rows_in_event_typ, 3) AS max_rows_in_event_type_event_at_tercile_percent_,
        ROUND(CUME_DIST() OVER w_distinct_event_types_by_last, 3) AS last_second_event_at_cume_dist
    FROM stats
    WINDOW w_all_by_max_rows_in_event_typ AS (ORDER BY max_rows_in_event_type_event_at_tercile), w_distinct_event_types_by_last AS (PARTITION BY distinct_event_types ORDER BY last_second_event_at DESC)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked_2
    WHERE max_rows_in_event_type_event_at_tercile_percent_ > (SELECT AVG(max_rows_in_event_type_event_at_tercile_percent_) FROM ranked_2)
)
-- Final projection
SELECT
    second_event_at_day,
    distinct_event_types,
    TO_DATE(last_second_event_at) AS last_second_event_at_date
FROM without_matches
ORDER BY distinct_event_types DESC, second_event_at_day DESC
LIMIT 10
