WITH filtered_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT_WS(' / ', country, artist_name) AS country_artist_name_label
    FROM artists
    WHERE LOWER(artist_name) LIKE '%ht%'
      AND is_verified = FALSE
)
SELECT debut_year, genre AS category
FROM filtered_artists
WHERE is_verified = FALSE
  AND genre <> 'Indie'
EXCEPT ALL
SELECT track_id AS debut_year, device AS category
FROM plays
WHERE listener_id >= 3
  AND source = 'playlist'
