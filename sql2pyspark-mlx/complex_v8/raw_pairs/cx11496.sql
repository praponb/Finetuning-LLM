WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
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
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        warehouse_id,
        ROUND(100 * COUNT(CASE WHEN move_type NOT IN ('INBOUND', 'TRANSFER') THEN 1 END) / COUNT(*), 1) AS pct_move_type_not_in_inbound_tra,
        COUNT(*) AS cnt,
        SUM(unit_price) AS total_unit_price,
        MIN(qty) AS min_qty
    FROM cleaned_moves
    GROUP BY 1, 2
),
enriched AS (
    -- Label and score rows
    SELECT
        operator,
        warehouse_id,
        cnt,
        total_unit_price,
        min_qty,
        ABS(cnt - 8) AS cnt_distance,
        ROUND(total_unit_price * 1.1, 2) AS total_unit_price_adj
    FROM grouped
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        operator,
        warehouse_id,
        cnt,
        total_unit_price,
        min_qty,
        cnt_distance,
        total_unit_price_adj,
        SUM(cnt) OVER (PARTITION BY cnt_distance, warehouse_id ORDER BY min_qty DESC NULLS FIRST, operator, warehouse_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt_sum_2,
        LEAD(cnt) OVER w_cnt_distance_warehouse_id_by_2 AS next_cnt,
        LAG(total_unit_price_adj) OVER (PARTITION BY cnt_distance, warehouse_id ORDER BY min_qty DESC NULLS FIRST, operator, warehouse_id) AS prev_total_unit_price_adj,
        min_qty - LAG(min_qty) OVER w_cnt_distance_warehouse_id_by_2 AS min_qty_change,
        SUM(total_unit_price_adj) OVER w_operator AS operator_sum_total_unit_price_adj
    FROM enriched
    WINDOW w_cnt_distance_warehouse_id_by_2 AS (PARTITION BY cnt_distance, warehouse_id ORDER BY min_qty DESC NULLS FIRST, operator, warehouse_id), w_operator AS (PARTITION BY operator)
)
SELECT
    operator,
    warehouse_id,
    total_unit_price,
    min_qty,
    cnt_distance,
    cnt_sum_2,
    next_cnt,
    prev_total_unit_price_adj
FROM with_trends
ORDER BY min_qty
