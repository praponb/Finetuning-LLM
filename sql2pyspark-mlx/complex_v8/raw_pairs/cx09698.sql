SELECT id
FROM skus
WHERE category = 'Beauty'
  AND is_active = FALSE
EXCEPT
SELECT id
FROM skus
WHERE is_active = TRUE
ORDER BY id DESC
