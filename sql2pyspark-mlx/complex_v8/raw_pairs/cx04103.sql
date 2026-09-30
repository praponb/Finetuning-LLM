WITH stacked AS (
    -- Stack the branches
    SELECT qty AS ref_id, unit_price AS value
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND MONTH(moved_at) IN (2, 3)
    UNION
    SELECT id, unit_price
    FROM stock_moves
    WHERE unit_price > 215.4
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
