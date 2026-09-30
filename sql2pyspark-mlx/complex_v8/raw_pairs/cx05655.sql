SELECT debut_year
FROM artists
EXCEPT
SELECT bpm AS debut_year
FROM tracks
WHERE SIZE(moods) >= 1
  AND LENGTH(TRIM(title)) >= 4
