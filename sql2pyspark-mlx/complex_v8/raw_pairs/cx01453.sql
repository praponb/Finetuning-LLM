SELECT category AS name, id AS key_id, unit_cost AS metric
FROM skus
WHERE unit_cost <= 277.0
UNION
SELECT operator, sku_id, unit_price
FROM stock_moves
