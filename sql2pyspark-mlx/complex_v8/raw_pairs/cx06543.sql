WITH prepared_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year
    FROM artists
    WHERE country IN ('US', 'BR', 'KR')
      AND debut_year < 2015
)
SELECT genre AS label, artist_name AS label_2, id
FROM prepared_artists
UNION ALL
SELECT artist_name, genre, debut_year
FROM artists
UNION ALL
SELECT device, source, track_id
FROM plays
WHERE ms_played BETWEEN 97825 AND 301865
ORDER BY label_2, label
