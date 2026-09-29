SELECT id, signup_date, 'listener' AS src
FROM listeners
WHERE country IN ('BR', 'TH')
  AND UPPER(TRIM(username)) LIKE 'D%'
UNION
SELECT duration_sec AS id, release_date AS signup_date, 'track' AS src
FROM tracks
WHERE release_date >= DATE '2024-11-14'
ORDER BY id DESC, src
