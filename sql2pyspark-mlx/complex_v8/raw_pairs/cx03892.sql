WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        ABS(duration_sec - 184) AS duration_sec_distance
    FROM tracks
    WHERE artist_id <= 5
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.duration_sec_distance,
        a.debut_year AS artist_debut_year
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year >= 1997
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(duration_sec_distance, 0) AS duration_sec_distance_filled
    FROM with_details_artist
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    duration_sec_distance,
    artist_debut_year,
    duration_sec_distance_filled
FROM tagged
WHERE artist_id >= 7
ORDER BY id DESC
