WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM plays
    WHERE (is_skipped = TRUE OR ms_played <= 302279)
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at < TIMESTAMP '2025-02-24 11:00:00'
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        is_skipped,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        SUM(ms_played) FILTER (WHERE source = 'search') AS ms_played_source_search
    FROM screened
    WHERE source_ref IS NOT NULL
    GROUP BY is_skipped, DATE_TRUNC('MONTH', played_at)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(first_played_at) OVER (PARTITION BY is_skipped ORDER BY ms_played_source_search DESC, is_skipped, played_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_played_at,
        LEAD(first_played_at, 3) OVER w_is_skipped_by_ms_played_sour_2 AS next3_first_played_at,
        DENSE_RANK() OVER w_all_by_ms_played_source_sear AS ms_played_source_search_dense_rank,
        SUM(distinct_listener_ids) OVER (PARTITION BY distinct_listener_ids) AS distinct_listener_ids_sum_distinct_listener_ids
    FROM grouped
    WINDOW w_is_skipped_by_ms_played_sour_2 AS (PARTITION BY is_skipped ORDER BY ms_played_source_search DESC, is_skipped, played_at_month), w_all_by_ms_played_source_sear AS (ORDER BY ms_played_source_search NULLS LAST)
)
-- Final projection
SELECT
    is_skipped,
    played_at_month,
    first_played_at,
    last_played_at,
    distinct_listener_ids,
    distinct_track_ids,
    median_ms_played,
    ms_played_source_search,
    last_first_played_at,
    next3_first_played_at,
    ms_played_source_search_dense_rank,
    distinct_listener_ids_sum_distinct_listener_ids,
    ROUND(ms_played_source_search / NULLIF(median_ms_played, 0), 3) AS ms_played_source_search_per_median_ms_played
FROM sequenced
ORDER BY first_played_at DESC, last_played_at DESC
