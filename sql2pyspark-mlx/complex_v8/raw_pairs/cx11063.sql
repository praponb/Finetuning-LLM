SELECT source AS name
FROM plays
WHERE source IN ('playlist', 'search')
  AND track_id <= 2
INTERSECT
SELECT title AS name
FROM tracks
