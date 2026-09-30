WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm
    FROM tracks
    WHERE artist_id <= 14
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.debut_year AS artist_debut_year, a.artist_name
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year <= 2006
)
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    bpm,
    least_duration_sec_bpm,
    artist_debut_year,
    artist_name
FROM with_parent_artist
ORDER BY artist_debut_year DESC, id NULLS LAST
