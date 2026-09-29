WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE duration_sec <= 402
      AND bpm >= 104
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM prepared_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'sad')
        )
),
without_matches_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'sad')
              AND t.release_date BETWEEN DATE '2024-06-03' AND DATE '2025-11-14'
        )
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    title_ref,
    release_date_label,
    duration_sec_bucket,
    LOWER(REGEXP_REPLACE(TRIM(title_ref), ' ', '_')) AS title_ref_slug
FROM without_matches_2
ORDER BY duration_sec_bucket, duration_sec
