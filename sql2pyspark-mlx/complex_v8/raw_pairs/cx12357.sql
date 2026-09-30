WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        moved_at,
        unit_price,
        RANK() OVER w_warehouse_id_by_unit_price AS unit_price_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC NULLS FIRST, id), 3) AS moved_at_percent_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY warehouse_id, TO_DATE(moved_at) ORDER BY qty DESC, id) AS unit_price_change,
        ROUND(unit_price - AVG(unit_price) OVER w_operator_warehouse_id, 2) AS unit_price_vs_operator_warehouse_id_avg,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, warehouse_id), 2) AS qty_vs_sku_id_warehouse_id_avg,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        LEAD(qty, 3, 0) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS next3_qty,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER w_operator, 0), 4) AS qty_share_of_operator,
        DENSE_RANK() OVER w_warehouse_id_sku_id_by_moved AS moved_at_dense_rank,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator), 2) AS unit_price_vs_operator_avg
    FROM valid_moves
    WINDOW w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price DESC, id), w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id), w_operator AS (PARTITION BY operator), w_warehouse_id_sku_id_by_moved AS (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC NULLS FIRST, id)
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        moved_at_dense_rank,
        sku_id,
        MIN(running_unit_price) AS min_running_unit_price,
        MIN(moved_at) AS first_moved_at,
        ROUND(SUM(unit_price_vs_operator_warehouse_id_avg) / NULLIF(SUM(moved_at_percent_rank), 0), 3) AS unit_price_vs_operator_warehouse_id_avg_to_moved
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', moved_at), moved_at_dense_rank, sku_id
)
-- Final projection
SELECT
    moved_at_month,
    moved_at_dense_rank,
    sku_id,
    min_running_unit_price,
    unit_price_vs_operator_warehouse_id_avg_to_moved
FROM stats
WHERE unit_price_vs_operator_warehouse_id_avg_to_moved < 7.5
ORDER BY moved_at_dense_rank
