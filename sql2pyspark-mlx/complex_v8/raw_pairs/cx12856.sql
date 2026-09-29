SELECT source AS label, device
FROM plays
WHERE HOUR(played_at) BETWEEN 7 AND 12
  AND is_skipped = TRUE
UNION ALL
SELECT plan, username
FROM listeners
ORDER BY device DESC
