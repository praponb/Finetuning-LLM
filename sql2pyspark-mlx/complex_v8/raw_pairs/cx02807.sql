SELECT sku_id, operator AS label, unit_price
FROM stock_moves
WHERE moved_at < TIMESTAMP '2025-03-02 10:00:00'
UNION
SELECT id, sku_code, weight_kg
FROM skus
WHERE unit_cost < 251.2
  AND UPPER(TRIM(sku_code)) LIKE 'E%'
ORDER BY label, unit_price
