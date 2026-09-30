WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id <= 4
      AND title IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.artist_name,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND HOUR(p.played_at) BETWEEN 9 AND 15
              AND NOT p.is_skipped
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(artist_debut_year, 1, 0) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY artist_debut_year DESC, id) AS next_artist_debut_year,
        MIN(artist_debut_year) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_artist_debut_year_so_far
    FROM without_matches
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    moods,
    artist_debut_year,
    min_artist_debut_year_so_far
FROM sequenced
ORDER BY artist_debut_year DESC NULLS FIRST, id DESC NULLS FIRST
