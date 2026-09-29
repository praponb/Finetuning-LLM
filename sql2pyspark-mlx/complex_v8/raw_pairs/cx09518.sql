WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        moved_at,
        operator,
        qty_plus_unit_price,
        SUM(qty_plus_unit_price) OVER (ORDER BY moved_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS qty_plus_unit_price_sum_6_prior,
        LAST_VALUE(unit_price) OVER (PARTITION BY sku_id, warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty_plus_unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        LEAD(moved_at, 2) OVER (PARTITION BY operator, sku_id ORDER BY qty ASC, id) AS next2_moved_at,
        MAX(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty_plus_unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_max_6_prior,
        LEAD(qty_plus_unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty_plus_unit_price DESC, id) AS next_qty_plus_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_operator, 0), 4) AS unit_price_share_of_sku_id_operator,
        SIZE(COLLECT_SET(warehouse_id) OVER w_operator) AS distinct_warehouse_id_in_operator,
        NTILE(5) OVER (PARTITION BY sku_id, operator ORDER BY unit_price DESC, id) AS unit_price_quintile,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY qty_plus_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        LEAD(qty_plus_unit_price) OVER (PARTITION BY sku_id, operator ORDER BY unit_price DESC, id) AS next_qty_plus_unit_price_2,
        SUM(qty_plus_unit_price) OVER (PARTITION BY operator) AS operator_sum_qty_plus_unit_price,
        DENSE_RANK() OVER w_sku_id_operator_by_unit_pric_2 AS unit_price_dense_rank,
        ROUND(qty_plus_unit_price - AVG(qty_plus_unit_price) OVER (PARTITION BY sku_id, warehouse_id), 2) AS qty_plus_unit_price_vs_sku_id_warehouse_id_avg
    FROM base_moves
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator), w_operator AS (PARTITION BY operator), w_sku_id_operator_by_unit_pric_2 AS (PARTITION BY sku_id, operator ORDER BY unit_price DESC)
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    qty_plus_unit_price,
    unit_price_avg_1_prior,
    next_qty_plus_unit_price,
    distinct_warehouse_id_in_operator,
    unit_price_quintile,
    second_moved_at,
    next_qty_plus_unit_price_2,
    operator_sum_qty_plus_unit_price,
    unit_price_dense_rank,
    DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
    CASE WHEN last_unit_price >= 313.8 THEN 'A' WHEN last_unit_price >= 160.8 THEN 'B' ELSE 'C' END AS last_unit_price_tier
FROM metrics
ORDER BY qty_plus_unit_price DESC
