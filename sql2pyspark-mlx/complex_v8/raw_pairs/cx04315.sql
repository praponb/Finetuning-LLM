SELECT id
FROM tracks
WHERE is_explicit = FALSE
EXCEPT ALL
SELECT duration_sec AS id
FROM tracks
WHERE UPPER(TRIM(title)) LIKE 'G%'
  AND duration_sec > 183
ORDER BY id DESC
