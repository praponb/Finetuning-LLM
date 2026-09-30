WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'search'
      AND track_id IS NOT NULL
),
grouped AS (
    -- Summarise per group
    SELECT
        listener_id,
        source,
        TO_DATE(played_at) AS played_at_day,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned_plays
    WHERE is_skipped = TRUE
    GROUP BY listener_id, source, TO_DATE(played_at)
    HAVING AVG(ms_played) > 107912
),
derived AS (
    -- Derive helper columns
    SELECT
        listener_id,
        source,
        played_at_day,
        stddev_ms_played,
        DATE_TRUNC('YEAR', played_at_day) AS played_at_day_year,
        DATEDIFF(DATE '2025-12-31', played_at_day) AS days_since_played_at_day,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM grouped
    WHERE distinct_ids > 5
)
-- Final projection
SELECT
    listener_id,
    source,
    played_at_day_year,
    days_since_played_at_day,
    source_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(source_or_unknown), ' ', '_')) AS source_or_unknown_slug,
    ROUND(stddev_ms_played * 1.1, 2) AS stddev_ms_played_adj
FROM derived
ORDER BY days_since_played_at_day DESC NULLS FIRST, source NULLS LAST
