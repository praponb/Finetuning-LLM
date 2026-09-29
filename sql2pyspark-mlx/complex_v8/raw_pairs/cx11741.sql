SELECT country AS category
FROM artists
EXCEPT ALL
SELECT artist_name AS category
FROM artists
ORDER BY category
