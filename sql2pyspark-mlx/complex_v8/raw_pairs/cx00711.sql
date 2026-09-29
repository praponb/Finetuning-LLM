WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE (operator = 'op_dew' OR MONTH(moved_at) IN (2, 3))
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.category AS sku_category
    FROM base_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        sku_category,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type_operator) AS distinct_warehouse_id_in_move_type_operator,
        MAX(qty) OVER (PARTITION BY warehouse_id, sku_category) AS warehouse_id_sku_category_max_qty,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY qty NULLS LAST, id) AS qty_quintile,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_category), 2) AS unit_price_vs_warehouse_id_sku_category_avg,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        FIRST_VALUE(unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROW_NUMBER() OVER (ORDER BY qty, id) AS rn,
        LAG(unit_price) OVER (ORDER BY qty, id) AS prev_unit_price
    FROM enriched
    WINDOW w_move_type_operator AS (PARTITION BY move_type, operator)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN sku_category IN ('Beauty', 'Food') THEN 'group_a' WHEN sku_category = 'Electronics' THEN 'group_b' ELSE 'other' END AS sku_category_group,
        prev_unit_price - qty_quintile AS prev_unit_price_minus_qty_quintile,
        SUBSTRING(TRIM(sku_category), 1, 2) AS sku_category_prefix
    FROM sequenced
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        sku_category,
        warehouse_id_sku_category_max_qty,
        unit_price_vs_warehouse_id_sku_category_avg,
        unit_price_avg_6_prior,
        first_unit_price,
        rn,
        prev_unit_price,
        prev_unit_price_minus_qty_quintile,
        MAX(first_unit_price) OVER (PARTITION BY sku_category, sku_category_group) AS sku_category_sku_category_group_max_first_unit_p,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_category, sku_category_group), 2) AS sku_category_sku_category_group_avg_unit_price,
        ROW_NUMBER() OVER (ORDER BY prev_unit_price_minus_qty_quintile NULLS LAST, id) AS rn_2,
        DENSE_RANK() OVER (ORDER BY prev_unit_price_minus_qty_quintile NULLS LAST, id) AS prev_unit_price_minus_qty_quintile_dense_rank,
        NTILE(10) OVER w_sku_category_rn_by_distinct AS distinct_warehouse_id_in_move_type_operator_deci,
        NTILE(3) OVER (ORDER BY prev_unit_price_minus_qty_quintile NULLS LAST, id) AS prev_unit_price_minus_qty_quintile_tercile
    FROM labeled
    WINDOW w_sku_category_rn_by_distinct AS (PARTITION BY sku_category, rn ORDER BY distinct_warehouse_id_in_move_type_operator ASC NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    sku_category,
    warehouse_id_sku_category_max_qty,
    unit_price_vs_warehouse_id_sku_category_avg,
    unit_price_avg_6_prior,
    first_unit_price,
    rn,
    prev_unit_price,
    prev_unit_price_minus_qty_quintile,
    sku_category_sku_category_group_max_first_unit_p,
    sku_category_sku_category_group_avg_unit_price,
    rn_2,
    prev_unit_price_minus_qty_quintile_dense_rank,
    distinct_warehouse_id_in_move_type_operator_deci,
    prev_unit_price_minus_qty_quintile_tercile,
    LOWER(REGEXP_REPLACE(TRIM(sku_category), ' ', '_')) AS sku_category_slug,
    qty + unit_price_avg_6_prior AS qty_plus_unit_price_avg_6_prior
FROM ranked
ORDER BY rn, unit_price_avg_6_prior DESC, id
LIMIT 5
