WITH stacked AS (
    -- Stack the branches
    SELECT qty AS ref_id, sku_id AS qty, unit_price AS metric
    FROM stock_moves
    UNION ALL
    SELECT qty AS ref_id, id AS qty, unit_price AS metric
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
      AND qty > 109
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
