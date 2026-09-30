SELECT title AS label, release_date AS day, duration_sec AS qty
FROM tracks
UNION ALL
SELECT title AS label, release_date AS day, id AS qty
FROM tracks
ORDER BY qty
