SELECT device
FROM plays
WHERE HOUR(played_at) BETWEEN 8 AND 13
EXCEPT
SELECT artist_name
FROM artists
WHERE UPPER(TRIM(artist_name)) LIKE 'K%'
