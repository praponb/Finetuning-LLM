WITH stacked AS (
    -- Stack the branches
    SELECT move_type AS label, unit_price AS metric, 'move' AS source
    FROM stock_moves
    UNION
    SELECT operator AS label, unit_price AS metric, 'move_2' AS source
    FROM stock_moves
    WHERE unit_price >= 60.4
      AND sku_id >= 7
)
SELECT source, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM stacked
GROUP BY source
ORDER BY source
