SELECT id AS qty, source
FROM plays
EXCEPT ALL
SELECT id, title
FROM tracks
WHERE release_date >= DATE '2024-09-26'
  AND LOWER(title) LIKE '%ri%'
ORDER BY source, qty
