SELECT id, artist_name
FROM artists
WHERE genre <> 'Pop'
EXCEPT
SELECT id, device AS artist_name
FROM plays
ORDER BY artist_name, id
