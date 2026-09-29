SELECT duration_sec AS key_id
FROM tracks
WHERE bpm BETWEEN 75 AND 143
EXCEPT
SELECT bpm
FROM tracks
WHERE ARRAY_CONTAINS(moods, 'focus')
