WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
    WHERE artist_id <= 13
      AND bpm < 143
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM cleaned_tracks
    WHERE bpm_per_duration_sec < (SELECT MAX(bpm_per_duration_sec) FROM cleaned_tracks)
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm_per_duration_sec,
        s.days_since_release_date,
        t.mood
    FROM screened s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM per_label s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.played_at < TIMESTAMP '2025-02-24 12:00:00'
        )
),
filtered_2 AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.debut_year < 2006
        )
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm_per_duration_sec,
        s.days_since_release_date,
        s.mood,
        a.is_verified AS artist_is_verified,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM filtered_2 s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    release_date,
    bpm_per_duration_sec,
    days_since_release_date,
    artist_is_verified,
    artist_name
FROM enriched_artist
ORDER BY artist_id DESC, bpm_per_duration_sec DESC
