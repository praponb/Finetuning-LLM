SELECT country AS label
FROM listeners
WHERE YEAR(signup_date) = 2024
EXCEPT
SELECT title
FROM tracks
WHERE bpm < 166
  AND artist_id IS NOT NULL
ORDER BY label DESC
