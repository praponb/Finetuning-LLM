SELECT debut_year, country
FROM artists
WHERE genre = 'Classical'
UNION ALL
SELECT listener_id, device
FROM plays
WHERE listener_id <= 9
ORDER BY country, debut_year
