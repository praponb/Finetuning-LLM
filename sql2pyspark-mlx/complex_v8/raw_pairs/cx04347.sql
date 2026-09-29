WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE unit_price >= 124.1
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
per_group AS (
    -- Summarise per group
    SELECT
        sku_category,
        operator,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        ARRAY_SORT(COLLECT_SET(sku_category)) AS sku_category_list,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        COUNT(DISTINCT sku_category) AS distinct_sku_categorys
    FROM joined_sku
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        sku_category,
        operator,
        n_rows,
        sku_code_list,
        distinct_warehouse_ids,
        distinct_sku_categorys,
        ROUND(AVG(distinct_sku_categorys) OVER (PARTITION BY sku_category, distinct_sku_categorys ORDER BY distinct_warehouse_ids, sku_category, operator ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_sku_categorys_avg_2_prior,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM per_group
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, distinct_sku_categorys, operator, distinct_sku_categorys_avg_2_prior
    FROM with_windows
) src
PIVOT (
    COUNT(distinct_sku_categorys_avg_2_prior)
    FOR operator IN ('op_ann', 'op_chan', 'op_bee', 'op_eve')
)
ORDER BY n_rows, distinct_sku_categorys
