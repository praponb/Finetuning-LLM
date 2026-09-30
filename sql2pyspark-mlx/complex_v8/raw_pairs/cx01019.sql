WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE is_skipped = TRUE
),
totals AS (
    -- Summarise per group
    SELECT
        source,
        is_skipped,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE source = 'radio') AS n_source_radio,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        COUNT(DISTINCT source) AS distinct_sources
    FROM base
    GROUP BY source, is_skipped
),
windowed AS (
    -- Previous / next values per key
    SELECT
        source,
        is_skipped,
        source_list,
        row_count,
        n_source_radio,
        FIRST_VALUE(distinct_sources) OVER (PARTITION BY source, distinct_track_ids ORDER BY row_count, source, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_sources,
        MAX(n_source_radio) OVER (PARTITION BY source, distinct_track_ids ORDER BY row_count, source, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_source_radio_so_far
    FROM totals
)
-- Final projection
SELECT
    source,
    is_skipped,
    source_list,
    row_count,
    n_source_radio,
    first_distinct_sources,
    max_n_source_radio_so_far
FROM windowed
ORDER BY max_n_source_radio_so_far NULLS LAST, source, is_skipped
LIMIT 20
