SELECT id, debut_year, country AS name
FROM artists
WHERE country IN ('JP', 'KR', 'BR')
  AND LOWER(artist_name) LIKE '%a %'
UNION ALL
SELECT artist_id AS id, id AS debut_year, title AS name
FROM tracks
WHERE duration_sec >= 165
  AND LENGTH(TRIM(title)) >= 5
UNION ALL
SELECT artist_id AS id, id AS debut_year, title AS name
FROM tracks
ORDER BY debut_year DESC, id
