WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND LENGTH(TRIM(title)) >= 4
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.duration_sec_minus_bpm,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year <= 2015
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(duration_sec - 213) AS duration_sec_distance,
        SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix
    FROM combined_details_artist
    WHERE UPPER(TRIM(artist_name)) LIKE 'K%'
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played
    FROM labeled s
)
SELECT
    is_explicit,
    artist_id,
    GROUPING(is_explicit) AS is_total_is_explicit,
    SUM(duration_sec_distance) FILTER (WHERE NOT is_explicit) AS duration_sec_distance_not_is_explicit,
    COUNT(*) AS row_count
FROM with_stats
GROUP BY CUBE(is_explicit, artist_id)
