SELECT category
FROM skus
EXCEPT ALL
SELECT category
FROM skus
WHERE unit_cost <= 306.4
  AND is_active = TRUE
ORDER BY category
