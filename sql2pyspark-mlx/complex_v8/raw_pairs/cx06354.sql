WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_ann', 'op_bee')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator)) AS distinct_warehouse_id_in_operator,
        NTILE(3) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty DESC, id) AS qty_tercile,
        MIN(unit_price) OVER (PARTITION BY operator) AS operator_min_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER w_operator, 0), 4) AS qty_share_of_operator,
        COUNT(*) OVER w_operator AS rows_in_operator,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY qty) AS qty_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY qty DESC, id), 3) AS qty_percent_rank,
        NTILE(4) OVER (PARTITION BY sku_id ORDER BY unit_price, id) AS unit_price_quartile,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator), 2) AS unit_price_vs_operator_avg
    FROM base_moves
    WINDOW w_operator AS (PARTITION BY operator)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        unit_price_quartile,
        SUM(qty) AS total_qty,
        ROUND(100 * COUNT(CASE WHEN operator <> 'op_bee' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_bee,
        MIN(unit_price) AS min_unit_price
    FROM ranked
    GROUP BY unit_price_quartile
)
-- Final projection
SELECT
    unit_price_quartile,
    min_unit_price,
    ROUND(unit_price_quartile / NULLIF(min_unit_price, 0), 3) AS unit_price_quartile_per_min_unit_price
FROM rollup_base
WHERE min_unit_price < 325.4
