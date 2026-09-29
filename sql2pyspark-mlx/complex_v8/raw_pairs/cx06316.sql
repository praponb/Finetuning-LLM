SELECT artist_id
FROM tracks
EXCEPT
SELECT artist_id
FROM tracks
WHERE is_explicit = TRUE
ORDER BY artist_id
