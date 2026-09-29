WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE release_date < DATE '2024-12-22'
      AND duration_sec < 353
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM base_tracks s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.duration_sec_per_bpm,
        s.bpm_bucket,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    is_explicit,
    artist_id,
    ROUND(AVG(duration_sec_per_bpm), 2) AS avg_duration_sec_per_bpm,
    COUNT(*) AS n_rows,
    MIN(duration_sec_per_bpm) AS min_duration_sec_per_bpm
FROM with_parent
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
