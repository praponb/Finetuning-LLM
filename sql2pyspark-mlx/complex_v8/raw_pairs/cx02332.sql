SELECT id, title AS label
FROM tracks
WHERE artist_id <= 11
EXCEPT ALL
SELECT debut_year, artist_name
FROM artists
WHERE country NOT IN ('US', 'JP')
  AND LOWER(artist_name) LIKE '% g%'
ORDER BY id DESC, label
