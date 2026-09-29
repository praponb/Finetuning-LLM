WITH stacked AS (
    -- Stack the branches
    SELECT warehouse_id, unit_price AS metric
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND move_type <> 'ADJUST'
    UNION ALL
    SELECT id AS warehouse_id, unit_cost AS metric
    FROM skus
    WHERE NOT ARRAY_CONTAINS(labels, 'hazmat')
      AND category NOT IN ('Toys', 'Electronics')
    UNION ALL
    SELECT sku_id AS warehouse_id, unit_price AS metric
    FROM stock_moves
    WHERE warehouse_id >= 4
      AND qty < 370
)
SELECT warehouse_id, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM stacked
GROUP BY warehouse_id
ORDER BY warehouse_id
