WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = TRUE
        )
      AND s.artist_id IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        duration_sec,
        release_date,
        moods,
        release_date_month,
        duration_sec_bucket,
        ABS(duration_sec - 209) AS duration_sec_distance,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM screened
)
-- Final projection
SELECT
    id,
    moods,
    duration_sec_distance,
    title_prefix,
    FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket_2
FROM tagged
WHERE ARRAY_CONTAINS(moods, 'sad')
ORDER BY duration_sec_distance, id NULLS LAST
