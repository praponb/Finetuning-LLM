SELECT source, device AS category, ms_played
FROM plays
UNION
SELECT artist_name, genre, debut_year
FROM artists
ORDER BY source, category
