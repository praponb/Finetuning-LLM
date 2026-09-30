SELECT title AS name
FROM tracks
EXCEPT
SELECT title
FROM tracks
ORDER BY name DESC
