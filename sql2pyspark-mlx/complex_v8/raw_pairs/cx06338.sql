SELECT country AS name
FROM listeners
WHERE signup_date BETWEEN DATE '2024-03-16' AND DATE '2025-01-29'
INTERSECT
SELECT artist_name AS name
FROM artists
WHERE genre = 'Jazz'
