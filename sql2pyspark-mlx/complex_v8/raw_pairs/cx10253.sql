WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id <= 5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        ROUND(AVG(qty) OVER (ORDER BY qty ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        ROUND(AVG(qty) OVER (ORDER BY qty ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS qty_avg_4
    FROM cleaned
)
SELECT id, metric, metric_value
FROM (
    SELECT id, qty_avg_4, qty_avg_3
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR metric IN (qty_avg_4 AS m_qty_avg_4, qty_avg_3 AS m_qty_avg_3)
) unp
ORDER BY id, metric
