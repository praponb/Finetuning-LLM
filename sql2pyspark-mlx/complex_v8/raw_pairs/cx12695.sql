SELECT id
FROM listeners
WHERE LENGTH(TRIM(username)) >= 6
  AND NOT ARRAY_CONTAINS(interests, 'jazz')
EXCEPT
SELECT birth_year
FROM listeners
ORDER BY id
