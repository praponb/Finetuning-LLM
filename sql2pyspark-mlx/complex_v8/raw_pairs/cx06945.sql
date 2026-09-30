WITH recent_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM artists
    WHERE debut_year < 2015
),
detail_rows AS (
    -- One row per track
    SELECT
        s.id,
        s.debut_year,
        s.is_verified,
        t.id AS track_id,
        t.bpm AS track_bpm,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date
    FROM recent_artists s
    JOIN tracks t ON t.artist_id = s.id
)
-- Final projection
SELECT
    id,
    debut_year,
    is_verified,
    track_moods,
    track_duration_sec,
    track_release_date,
    COALESCE(debut_year, 0) AS debut_year_filled,
    DATEDIFF(DATE '2026-01-01', track_release_date) AS days_since_track_release_date
FROM detail_rows
WHERE is_verified = TRUE
ORDER BY debut_year_filled DESC
