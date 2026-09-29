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
        ABS(unit_price - 264.8) AS unit_price_distance,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        LOWER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE operator NOT IN ('op_eve', 'op_chan')
      AND unit_price >= 111.7
),
qualified AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND UPPER(TRIM(w.wh_code)) LIKE 'B%'
        )
),
joined AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price_distance,
        s.moved_at_label,
        s.operator_clean,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM qualified s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category <> 'Toys'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        unit_price_distance - LAG(unit_price_distance) OVER w_warehouse_id_by_sku_unit_cos AS unit_price_distance_change,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY qty DESC) AS qty_dense_rank,
        ROUND(AVG(unit_price_distance) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price_distance,
        LAG(moved_at, 2) OVER w_all_by_moved_at AS prev2_moved_at
    FROM joined
    WINDOW w_warehouse_id_by_sku_unit_cos AS (PARTITION BY warehouse_id ORDER BY sku_unit_cost, id), w_all_by_moved_at AS (ORDER BY moved_at ASC, id)
)
SELECT id, measure, amount
FROM (
    SELECT id, move_type_avg_unit_price_distance, unit_price_distance_change, sku_unit_cost
    FROM sequenced
) src
UNPIVOT (
    amount FOR measure IN (move_type_avg_unit_price_distance AS move_type_avg_unit_price_distance_v, unit_price_distance_change AS m_unit_price_distance_change, sku_unit_cost AS sku_unit_cost_v)
) unp
