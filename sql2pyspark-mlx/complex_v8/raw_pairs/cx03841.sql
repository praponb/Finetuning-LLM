WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
      AND UPPER(TRIM(title)) LIKE 'E%'
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM recent s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year >= 1997
    WHERE a.is_verified = FALSE
)
SELECT id, field, reading
FROM (
    SELECT id, duration_sec, artist_debut_year
    FROM with_parent_artist
) src
UNPIVOT (
    reading FOR field IN (duration_sec AS duration_sec_v, artist_debut_year AS artist_debut_year_v)
) unp
ORDER BY id, field
