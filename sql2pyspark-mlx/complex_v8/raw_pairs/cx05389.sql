SELECT ms_played, device AS label
FROM plays
WHERE device <> 'phone'
  AND is_skipped = FALSE
INTERSECT
SELECT id AS ms_played, title AS label
FROM tracks
ORDER BY label
