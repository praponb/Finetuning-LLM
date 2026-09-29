WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE track_id >= 10
      AND ms_played <= 223262
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(ms_played - 99376) AS ms_played_distance,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        LOWER(TRIM(device_slug)) AS device_slug_clean
    FROM filtered_plays
    WHERE is_skipped = FALSE
),
totals AS (
    -- Group-level totals
    SELECT
        is_skipped,
        ROUND(AVG(ms_played), 2) AS avg_ms_played,
        ROUND(100 * COUNT(CASE WHEN source IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_source_is_not_null,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        ROUND(SUM(ms_played) / NULLIF(SUM(ms_played_distance), 0), 3) AS ms_played_to_ms_played_distance_ratio,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(DISTINCT source) AS distinct_sources
    FROM enriched
    WHERE source_slug IS NOT NULL
    GROUP BY is_skipped
    HAVING SUM(ms_played_distance) > 40000
)
-- Final projection
SELECT is_skipped, distinct_track_ids, ms_played_to_ms_played_distance_ratio
FROM totals
ORDER BY ms_played_to_ms_played_distance_ratio DESC NULLS FIRST, distinct_track_ids
