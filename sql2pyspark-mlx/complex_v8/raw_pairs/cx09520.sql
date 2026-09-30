SELECT country AS category, birth_year AS entity_id, plan
FROM listeners
UNION ALL
SELECT plan AS category, birth_year AS entity_id, country AS plan
FROM listeners
WHERE country <> 'TH'
  AND birth_year > 1975
ORDER BY category DESC
