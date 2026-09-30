SELECT id, birth_year, 'listener' AS record_type
FROM listeners
WHERE LOWER(username) LIKE '%_m%'
  AND signup_date >= DATE '2024-10-07'
UNION ALL
SELECT id, debut_year AS birth_year, 'artist' AS record_type
FROM artists
