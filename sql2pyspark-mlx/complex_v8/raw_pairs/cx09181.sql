SELECT id AS ref_id
FROM tracks
EXCEPT
SELECT id
FROM artists
WHERE is_verified = TRUE
  AND country <> 'KR'
ORDER BY ref_id DESC
