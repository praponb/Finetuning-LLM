SELECT title
FROM tracks
INTERSECT
SELECT device AS title
FROM plays
WHERE device = 'phone'
  AND NOT is_skipped
