SELECT listener_id, device
FROM plays
EXCEPT ALL
SELECT id, source
FROM plays
WHERE ms_played < 324437
  AND source IN ('radio', 'search', 'album')
ORDER BY listener_id DESC
