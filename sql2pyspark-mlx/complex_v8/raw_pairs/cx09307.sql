WITH prepared_artists AS (
    -- Filter artists
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 7
      AND is_verified = TRUE
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM prepared_artists s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.release_date IS NOT NULL
        )
),
line_level AS (
    -- One row per track
    SELECT
        s.id,
        s.debut_year,
        s.is_verified,
        s.debut_year_bucket,
        t.id AS track_id,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec
    FROM without_matches s
    JOIN tracks t ON t.artist_id = s.id AND t.duration_sec < 312
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND ARRAY_CONTAINS(t.moods, 'workout')
        )
      AND s.is_verified = TRUE
)
-- Final projection
SELECT
    id,
    debut_year,
    is_verified,
    debut_year_bucket,
    track_id,
    track_duration_sec,
    FLOOR(track_bpm / 25) * 25 AS track_bpm_bucket
FROM filtered
WHERE track_bpm BETWEEN 75 AND 142
ORDER BY debut_year, id, track_id
LIMIT 5
