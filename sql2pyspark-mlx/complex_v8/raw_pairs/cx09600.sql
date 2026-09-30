SELECT id AS key_id
FROM artists
WHERE country = 'JP'
EXCEPT
SELECT duration_sec
FROM tracks
WHERE duration_sec BETWEEN 164 AND 365
  AND bpm < 156
