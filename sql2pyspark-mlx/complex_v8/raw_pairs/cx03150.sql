SELECT birth_year
FROM listeners
WHERE plan = 'free'
EXCEPT
SELECT bpm
FROM tracks
WHERE bpm <= 147
