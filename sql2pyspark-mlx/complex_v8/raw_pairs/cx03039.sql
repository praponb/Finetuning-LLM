SELECT id, release_date
FROM tracks
UNION ALL
SELECT bpm, release_date
FROM tracks
UNION ALL
SELECT birth_year, signup_date
FROM listeners
WHERE country = 'BR'
  AND birth_year < 1994
ORDER BY release_date, id
