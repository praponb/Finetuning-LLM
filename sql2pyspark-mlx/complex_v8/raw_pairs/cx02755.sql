WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2006) AS debut_year_distance
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 5
      AND genre <> 'Indie'
),
combined AS (
    -- Stack the branches
    SELECT
        'artist' AS record_type,
        debut_year_distance AS entity_id,
        id AS entity_id_2,
        country
    FROM scoped
    WHERE country = 'US'
      AND debut_year <= 2015
    UNION ALL
    SELECT
        'track' AS record_type,
        id AS entity_id,
        duration_sec AS entity_id_2,
        title AS country
    FROM tracks
    WHERE artist_id <= 9
    UNION ALL
    SELECT
        'play' AS record_type,
        listener_id AS entity_id,
        track_id AS entity_id_2,
        source AS country
    FROM plays
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id_2) AS total_entity_id_2
FROM combined
GROUP BY record_type
ORDER BY record_type
