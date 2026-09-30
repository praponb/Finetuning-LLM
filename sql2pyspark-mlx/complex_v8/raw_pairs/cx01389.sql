SELECT sku_code AS category, unit_cost AS metric
FROM skus
UNION ALL
SELECT move_type AS category, unit_price AS metric
FROM stock_moves
WHERE HOUR(moved_at) BETWEEN 7 AND 12
ORDER BY category, metric
