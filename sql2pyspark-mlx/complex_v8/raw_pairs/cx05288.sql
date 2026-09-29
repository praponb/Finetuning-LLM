WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND artist_id IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket_2,
        ABS(duration_sec_bucket - 288) AS duration_sec_bucket_distance
    FROM scoped_tracks
    WHERE is_explicit_flag > 0
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM labeled s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    is_explicit_flag,
    release_date_month,
    first_played_at,
    ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
    ROUND(duration_sec / NULLIF(duration_sec_bucket, 0), 3) AS duration_sec_per_duration_sec_bucket
FROM summarized_rows
ORDER BY moods_text NULLS LAST, is_explicit_flag
