SELECT device AS name
FROM plays
INTERSECT ALL
SELECT username
FROM listeners
WHERE username IS NOT NULL
  AND plan = 'family'
ORDER BY name
