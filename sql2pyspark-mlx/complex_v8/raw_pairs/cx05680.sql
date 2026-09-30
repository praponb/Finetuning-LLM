WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-21' AND DATE '2025-11-29'
      AND is_explicit = FALSE
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.source = 'search' THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_source_count,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM filtered s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM with_counts s
    LEFT JOIN artists a ON a.id = s.artist_id
),
combined_details_2 AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.least_bpm_duration_sec,
        s.greatest_bpm_duration_sec,
        s.release_date_label,
        s.play_source_count,
        s.avg_play_ms_played,
        s.first_played_at,
        s.artist_debut_year,
        s.artist_genre,
        s.artist_is_verified,
        a.artist_name
    FROM combined_details s
    LEFT JOIN artists a ON a.id = s.artist_id
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM combined_details_2 s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.duration_sec BETWEEN 144 AND 409
        )
      AND s.artist_debut_year >= 1994
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.least_bpm_duration_sec,
        s.greatest_bpm_duration_sec,
        s.release_date_label,
        s.play_source_count,
        s.avg_play_ms_played,
        s.first_played_at,
        s.artist_debut_year,
        s.artist_genre,
        s.artist_is_verified,
        s.artist_name,
        t.mood
    FROM qualified s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    is_explicit,
    greatest_bpm_duration_sec,
    release_date_label,
    play_source_count,
    avg_play_ms_played,
    artist_genre,
    mood
FROM per_label
ORDER BY play_source_count, release_date NULLS LAST, id, mood
LIMIT 5
