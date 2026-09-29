SELECT genre, country AS category, 'artist' AS source
FROM artists
WHERE debut_year < 2015
UNION ALL
SELECT source, device, 'play' AS source
FROM plays
UNION ALL
SELECT username, country, 'listener' AS source
FROM listeners
WHERE LOWER(username) LIKE '%a9%'
  AND signup_date >= DATE '2024-10-07'
