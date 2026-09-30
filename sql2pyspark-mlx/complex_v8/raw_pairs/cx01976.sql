WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'OUTBOUND', 'INBOUND')
      AND warehouse_id >= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        qty,
        operator,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, warehouse_id), 0), 4) AS qty_share_of_operator_warehouse_id,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id)) AS distinct_warehouse_id_in_sku_id,
        ROUND(AVG(qty) OVER w_operator_warehouse_id, 2) AS operator_warehouse_id_avg_qty,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS seq_no,
        SUM(unit_price) OVER (PARTITION BY sku_id) AS sku_id_sum_unit_price,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SUM(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_sum_unit_price,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(AVG(qty) OVER w_sku_id, 2) AS sku_id_avg_qty,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) AS rn,
        unit_price - LAG(unit_price) OVER w_operator_warehouse_id_by_qty AS unit_price_change,
        COUNT(*) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(qty) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM recent_moves
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id), w_sku_id AS (PARTITION BY sku_id), w_operator_warehouse_id_by_qty AS (PARTITION BY operator, warehouse_id ORDER BY qty, id)
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    qty_share_of_operator_warehouse_id,
    distinct_warehouse_id_in_sku_id,
    operator_warehouse_id_avg_qty,
    seq_no,
    operator_warehouse_id_sum_unit_price,
    sku_id_avg_qty,
    rn,
    unit_price_change,
    running_count,
    running_qty,
    COALESCE(rn, 0) AS rn_filled
FROM sequenced
WHERE operator_warehouse_id_sum_unit_price < 3000.4
ORDER BY running_qty, id DESC
