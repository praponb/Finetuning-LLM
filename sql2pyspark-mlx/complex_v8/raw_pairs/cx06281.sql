WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        qty - unit_price AS qty_minus_unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE sku_id >= 15
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        qty_minus_unit_price,
        operator_slug,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY sku_id, operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        MAX(qty_minus_unit_price) OVER (ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_minus_unit_price_max_7,
        NTILE(4) OVER (PARTITION BY sku_id, operator ORDER BY qty DESC, id) AS qty_quartile,
        RANK() OVER (ORDER BY qty) AS qty_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id_operator) AS distinct_warehouse_id_in_sku_id_operator,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id, operator), 0), 4) AS unit_price_share_of_sku_id_operator
    FROM scoped
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.qty_minus_unit_price_max_7,
        s.qty_quartile,
        s.qty_rank,
        s.unit_price_share_of_sku_id_operator,
        sk.labels AS sku_labels
    FROM metrics s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.unit_cost < 320.4
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    qty_rank,
    sku_id,
    SUM(qty_quartile) AS total_qty_quartile
FROM with_parent_sku
GROUP BY ROLLUP(warehouse_id, qty_rank, sku_id)
