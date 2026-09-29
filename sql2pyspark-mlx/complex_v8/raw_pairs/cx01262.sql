WITH filtered_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'hazmat')
      AND category <> 'Beauty'
)
SELECT id, weight_kg AS metric, category AS name
FROM filtered_skus
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
  AND unit_cost < 302.5
UNION ALL
SELECT qty AS id, unit_price AS metric, operator AS name
FROM stock_moves
WHERE move_type = 'OUTBOUND'
  AND warehouse_id >= 2
UNION ALL
SELECT id, weight_kg AS metric, category AS name
FROM skus
ORDER BY id, metric
