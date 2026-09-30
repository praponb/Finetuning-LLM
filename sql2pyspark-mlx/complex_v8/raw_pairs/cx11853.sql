WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE qty > 102
      AND MONTH(moved_at) IN (1, 2)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS unit_price_vs_warehouse_id_avg,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id)) AS distinct_sku_id_in_warehouse_id
    FROM cleaned
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(operator_move_type_label), ' ', '_')) AS operator_move_type_label_slug,
        CASE WHEN unit_price >= 330.0 THEN 'large' WHEN unit_price >= 215.4 THEN 'medium' ELSE 'small' END AS unit_price_tier,
        LEAST(least_unit_price_qty, rows_in_warehouse_id) AS least_least_unit_price_qty_rows_in_warehouse_id
    FROM ranked
    WHERE qty <= 271
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.operator_move_type_label,
        s.least_unit_price_qty,
        s.unit_price_bucket,
        s.rows_in_warehouse_id,
        s.unit_price_vs_warehouse_id_avg,
        s.distinct_sku_id_in_warehouse_id,
        s.operator_move_type_label_slug,
        s.least_least_unit_price_qty_rows_in_warehouse_id,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        sk.sku_code
    FROM tagged s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty >= 78
              AND st.operator = 'op_ann'
        )
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY warehouse_capacity, id) AS seq_no,
        SUM(least_unit_price_qty) OVER (ORDER BY warehouse_capacity, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS least_unit_price_qty_sum_1_prior,
        RANK() OVER (ORDER BY warehouse_capacity) AS warehouse_capacity_rank
    FROM qualified
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM ranked_2 s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE MONTH(st.moved_at) IN (2, 3)
              AND st.qty >= 168
        )
)
SELECT id, measure, amount
FROM (
    SELECT id, rows_in_warehouse_id, seq_no, distinct_sku_id_in_warehouse_id, qty
    FROM screened
) src
UNPIVOT (
    amount FOR measure IN (rows_in_warehouse_id, seq_no, distinct_sku_id_in_warehouse_id, qty)
) unp
ORDER BY id, measure
