SELECT artist_id, release_date, 'track' AS record_type
FROM tracks
WHERE release_date IS NOT NULL
  AND bpm <= 142
UNION ALL
SELECT id, signup_date, 'listener' AS record_type
FROM listeners
UNION ALL
SELECT id, signup_date, 'listener' AS record_type
FROM listeners
WHERE UPPER(TRIM(username)) LIKE 'D%'
ORDER BY artist_id DESC
