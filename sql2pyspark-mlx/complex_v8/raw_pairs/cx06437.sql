WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_ann', 'op_bee') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND moved_at >= TIMESTAMP '2025-02-06 12:00:00'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM valid_moves
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator_group,
        s.operator_ref,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price
    FROM enriched s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM detail_rows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'INBOUND'
        )
      AND s.move_operator = 'op_bee'
),
per_group AS (
    -- Summarise per group
    SELECT
        move_id,
        move_operator,
        sku_id,
        COUNT(DISTINCT move_id) AS distinct_move_ids,
        SUM(move_unit_price) AS total_move_unit_price,
        SUM(CASE WHEN move_operator IN ('op_bee', 'op_ann') THEN 1 ELSE 0 END) AS n_move_operator_in_op_bee_op_a,
        SUM(qty) FILTER (WHERE move_operator IN ('op_dew', 'op_ann')) AS qty_move_operator_in_op_dew_op_a
    FROM without_matches
    WHERE move_id <= 66
    GROUP BY move_id, move_operator, sku_id
    HAVING SUM(move_unit_price) > 86.3
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        move_id,
        move_operator,
        sku_id,
        distinct_move_ids,
        total_move_unit_price,
        n_move_operator_in_op_bee_op_a,
        ROUND(AVG(total_move_unit_price) OVER (PARTITION BY n_move_operator_in_op_bee_op_a), 2) AS n_move_operator_in_op_bee_op_a_avg_total_move_un,
        LEAD(total_move_unit_price, 2, 0) OVER w_n_move_operator_in_op_bee_op_2 AS next2_total_move_unit_price,
        FIRST_VALUE(total_move_unit_price) IGNORE NULLS OVER (PARTITION BY n_move_operator_in_op_bee_op_a, move_operator ORDER BY total_move_unit_price, move_id, move_operator, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_move_unit_price,
        ROUND(distinct_move_ids - AVG(distinct_move_ids) OVER (PARTITION BY n_move_operator_in_op_bee_op_a), 2) AS distinct_move_ids_vs_n_move_operator_in_op_bee_o,
        LEAD(total_move_unit_price, 3) OVER w_n_move_operator_in_op_bee_op_2 AS next3_total_move_unit_price
    FROM per_group
    WINDOW w_n_move_operator_in_op_bee_op_2 AS (PARTITION BY n_move_operator_in_op_bee_op_a, move_operator ORDER BY total_move_unit_price, move_id, move_operator, sku_id)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.warehouse_id AS move_warehouse_id,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM metrics s
    JOIN stock_moves st ON st.id = s.move_id
    LEFT JOIN skus sk ON sk.id = st.sku_id
)
-- Final projection
SELECT
    move_id,
    move_operator,
    sku_id,
    distinct_move_ids,
    total_move_unit_price,
    n_move_operator_in_op_bee_op_a,
    n_move_operator_in_op_bee_op_a_avg_total_move_un,
    next2_total_move_unit_price,
    first_known_total_move_unit_price,
    distinct_move_ids_vs_n_move_operator_in_op_bee_o,
    next3_total_move_unit_price,
    move_warehouse_id,
    move_sku_id,
    move_qty,
    sku_unit_cost,
    SUBSTRING(TRIM(move_operator), 1, 4) AS move_operator_prefix
FROM with_details
