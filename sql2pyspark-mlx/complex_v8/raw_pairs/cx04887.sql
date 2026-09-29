SELECT id AS qty, source AS name
FROM plays
WHERE is_skipped = TRUE
  AND listener_id >= 17
EXCEPT
SELECT birth_year, country
FROM listeners
ORDER BY name, qty
