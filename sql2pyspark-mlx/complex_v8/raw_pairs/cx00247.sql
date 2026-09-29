SELECT country, username
FROM listeners
WHERE birth_year BETWEEN 1969 AND 1981
UNION
SELECT device, source
FROM plays
WHERE listener_id >= 3
  AND device IN ('phone', 'speaker')
ORDER BY country
