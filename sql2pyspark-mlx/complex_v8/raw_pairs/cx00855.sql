SELECT birth_year AS ref_id, username AS label
FROM listeners
INTERSECT
SELECT birth_year AS ref_id, username AS label
FROM listeners
WHERE country = 'BR'
  AND UPPER(TRIM(username)) LIKE 'E%'
ORDER BY ref_id DESC, label
