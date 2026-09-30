WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND sku_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior,
        MIN(qty) OVER w_sku_id_operator AS sku_id_operator_min_qty,
        ROUND(AVG(qty) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        MIN(unit_price) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_unit_price
    FROM latest
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    qty_avg_6_prior,
    sku_id_operator_min_qty,
    sku_id_operator_min_unit_price,
    CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group,
    FLOOR(sku_id_operator_min_qty / 1000) * 1000 AS sku_id_operator_min_qty_bucket
FROM windowed
ORDER BY sku_id_operator_min_qty_bucket, operator
