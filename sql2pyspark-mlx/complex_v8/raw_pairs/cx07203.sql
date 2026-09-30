WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(unit_price_plus_qty) OVER (PARTITION BY sku_id, operator), 2) AS sku_id_operator_avg_unit_price_plus_qty,
        qty - LAG(qty) OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id) AS qty_change,
        DENSE_RANK() OVER (PARTITION BY sku_id, operator ORDER BY unit_price) AS unit_price_dense_rank,
        DENSE_RANK() OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id) AS unit_price_dense_rank_2
    FROM without_matches
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT unit_price_dense_rank, warehouse_id, operator, unit_price_plus_qty
    FROM ranked
) src
PIVOT (
    MIN(unit_price_plus_qty)
    FOR operator IN ('op_dew', 'op_chan')
)
ORDER BY unit_price_dense_rank, warehouse_id
