WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec > 164
      AND SIZE(moods) >= 1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        a.debut_year AS artist_debut_year
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id AND a.artist_name IS NOT NULL
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id <= 6
              AND NOT p.is_skipped
        )
      AND SIZE(s.moods) >= 1
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.artist_debut_year,
        a.artist_name
    FROM screened s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods,
    artist_debut_year,
    artist_name,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM joined
