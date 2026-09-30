WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'E%'
      AND ARRAY_CONTAINS(moods, 'workout')
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.ms_played >= 72464
              AND p.source <> 'album'
        )
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        bpm_bucket,
        bpm_bucket - LAG(bpm_bucket) OVER (ORDER BY release_date DESC, id) AS bpm_bucket_change,
        MAX(bpm_bucket) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_bucket_max_3,
        ROUND(AVG(bpm_bucket) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS release_date_year_artist_id_avg_bpm_bucket
    FROM with_activity
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    moods,
    bpm_bucket_change,
    bpm_bucket_max_3,
    release_date_year_artist_id_avg_bpm_bucket,
    LEAST(bpm_bucket_max_3, duration_sec) AS least_bpm_bucket_max_3_duration_sec
FROM ranked
ORDER BY bpm_bucket_max_3 DESC, duration_sec NULLS LAST
