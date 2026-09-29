SELECT id, source
FROM plays
WHERE device = 'desktop'
  AND MONTH(played_at) IN (2, 3)
INTERSECT ALL
SELECT birth_year AS id, country AS source
FROM listeners
WHERE country IN ('TH', 'JP', 'BR')
  AND signup_date IS NOT NULL
ORDER BY id, source
