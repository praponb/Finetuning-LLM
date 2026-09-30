WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_ann', 'op_bee')
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM scoped_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id <= 10
        )
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_prefix,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM screened s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        move_id,
        move_unit_price,
        move_qty,
        SUM(move_unit_price) OVER (PARTITION BY TO_DATE(moved_at) ORDER BY move_qty, id, move_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS move_unit_price_sum_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY move_qty, id, move_id), 3) AS move_qty_cume_dist,
        SIZE(COLLECT_SET(move_id) OVER w_warehouse_id_sku_id) AS distinct_move_id_in_warehouse_id_sku_id
    FROM with_children
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id)
),
per_group AS (
    -- Summarise per group
    SELECT
        distinct_move_id_in_warehouse_id_sku_id,
        CASE WHEN move_qty >= 422 THEN 'A' WHEN move_qty >= 112 THEN 'B' ELSE 'C' END AS move_qty_tier,
        operator,
        COUNT(DISTINCT operator) AS distinct_operators,
        COUNT(*) AS n_rows,
        MAX(moved_at) AS last_moved_at
    FROM ranked
    GROUP BY distinct_move_id_in_warehouse_id_sku_id, CASE WHEN move_qty >= 422 THEN 'A' WHEN move_qty >= 112 THEN 'B' ELSE 'C' END, operator
),
enriched AS (
    -- Label and score rows
    SELECT
        distinct_move_id_in_warehouse_id_sku_id,
        move_qty_tier,
        operator,
        n_rows,
        last_moved_at,
        LEAST(distinct_operators, n_rows) AS least_distinct_operators_n_rows,
        n_rows - distinct_operators AS n_rows_minus_distinct_operators
    FROM per_group
    WHERE HOUR(last_moved_at) BETWEEN 6 AND 10
),
scored AS (
    -- Derive helper columns
    SELECT
        distinct_move_id_in_warehouse_id_sku_id,
        move_qty_tier,
        operator,
        n_rows,
        least_distinct_operators_n_rows,
        n_rows_minus_distinct_operators,
        INITCAP(TRIM(move_qty_tier)) AS move_qty_tier_clean
    FROM enriched
)
SELECT *
FROM (
    SELECT operator, move_qty_tier, least_distinct_operators_n_rows
    FROM scored
) src
PIVOT (
    AVG(least_distinct_operators_n_rows)
    FOR move_qty_tier IN ('A', 'C')
)
ORDER BY operator
