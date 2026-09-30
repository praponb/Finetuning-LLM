SELECT track_id, source AS category
FROM plays
WHERE is_skipped = TRUE
  AND track_id IS NOT NULL
EXCEPT
SELECT listener_id, device
FROM plays
WHERE listener_id IS NOT NULL
  AND ms_played BETWEEN 72464 AND 223828
ORDER BY track_id DESC
