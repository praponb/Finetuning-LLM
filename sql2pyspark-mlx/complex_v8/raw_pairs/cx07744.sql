WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE qty > 198
),
stacked AS (
    -- Stack the branches
    SELECT id, unit_price AS metric, qty
    FROM cleaned_moves
    UNION ALL
    SELECT sku_id AS id, unit_price AS metric, qty
    FROM stock_moves
    WHERE qty >= 90
    UNION ALL
    SELECT id, unit_price AS metric, warehouse_id AS qty
    FROM stock_moves
)
SELECT id, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM stacked
GROUP BY id
ORDER BY id
