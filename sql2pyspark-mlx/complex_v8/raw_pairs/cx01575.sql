SELECT duration_sec AS entity_id
FROM tracks
MINUS
SELECT id
FROM listeners
WHERE signup_date >= DATE '2024-10-17'
  AND country = 'KR'
