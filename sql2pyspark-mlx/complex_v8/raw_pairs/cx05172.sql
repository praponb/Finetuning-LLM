WITH cleaned AS (
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
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE qty > 116
      AND unit_price BETWEEN 95.8 AND 393.3
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
        s.moved_at_hour,
        sk.is_active AS sku_is_active,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM cleaned s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category IN ('Beauty', 'Apparel', 'Electronics')
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.is_active = FALSE
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        operator,
        warehouse_opened_on,
        ROW_NUMBER() OVER w_sku_is_active_warehouse_open AS seq_no,
        MAX(qty) OVER (PARTITION BY YEAR(moved_at) ORDER BY warehouse_capacity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_max_3_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator), 0), 4) AS qty_share_of_operator,
        DENSE_RANK() OVER w_sku_is_active_warehouse_open AS warehouse_capacity_dense_rank,
        ROUND(AVG(qty) OVER (PARTITION BY sku_is_active, YEAR(warehouse_opened_on) ORDER BY warehouse_capacity ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        ROUND(AVG(warehouse_capacity) OVER w_operator, 2) AS operator_avg_warehouse_capacity,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        LAG(unit_price) OVER w_operator_warehouse_id_by_war AS prev_unit_price,
        MAX(qty) OVER (PARTITION BY operator) AS operator_max_qty,
        LAG(moved_at_hour) OVER w_sku_is_active_warehouse_open AS prev_moved_at_hour,
        NTILE(3) OVER (PARTITION BY sku_is_active, YEAR(warehouse_opened_on) ORDER BY warehouse_capacity ASC, id) AS warehouse_capacity_tercile,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_is_active, YEAR(warehouse_opened_on) ORDER BY warehouse_capacity ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior
    FROM enriched
    WINDOW w_sku_is_active_warehouse_open AS (PARTITION BY sku_is_active, YEAR(warehouse_opened_on) ORDER BY warehouse_capacity ASC, id), w_operator AS (PARTITION BY operator), w_operator_warehouse_id_by_war AS (PARTITION BY operator, warehouse_id ORDER BY warehouse_capacity DESC, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        sku_id,
        operator,
        warehouse_opened_on,
        seq_no,
        qty_max_3_prior,
        qty_share_of_operator,
        qty_avg_2_prior,
        operator_avg_warehouse_capacity,
        rows_in_operator,
        prev_unit_price,
        operator_max_qty,
        prev_moved_at_hour,
        warehouse_capacity_tercile,
        unit_price_avg_1_prior
    FROM ranked
    WHERE warehouse_capacity_dense_rank <= 3
      AND operator_avg_warehouse_capacity < 16200.0
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        warehouse_opened_on,
        seq_no,
        qty_share_of_operator,
        qty_avg_2_prior,
        operator_avg_warehouse_capacity,
        prev_unit_price,
        operator_max_qty,
        prev_moved_at_hour,
        warehouse_capacity_tercile,
        unit_price_avg_1_prior,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        LEAD(prev_unit_price, 2, 0) OVER (PARTITION BY seq_no ORDER BY prev_moved_at_hour NULLS LAST, id) AS next2_prev_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no ORDER BY warehouse_opened_on NULLS LAST), 3) AS warehouse_opened_on_cume_dist,
        ROUND(qty_max_3_prior / NULLIF(SUM(qty_max_3_prior) OVER (PARTITION BY sku_id), 0), 4) AS qty_max_3_prior_share_of_sku_id,
        MIN(warehouse_capacity_tercile) OVER (PARTITION BY sku_id) AS sku_id_min_warehouse_capacity_tercile,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id)) AS distinct_operator_in_sku_id,
        MAX(prev_unit_price) OVER (PARTITION BY seq_no ORDER BY warehouse_opened_on NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_unit_price_max_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY seq_no ORDER BY prev_moved_at_hour NULLS LAST), 3) AS prev_moved_at_hour_percent_rank
    FROM top_rows
)
SELECT
    id,
    warehouse_opened_on,
    seq_no,
    qty_share_of_operator,
    qty_avg_2_prior,
    operator_avg_warehouse_capacity,
    prev_moved_at_hour,
    warehouse_capacity_tercile,
    unit_price_avg_1_prior,
    rows_in_sku_id,
    next2_prev_unit_price,
    warehouse_opened_on_cume_dist,
    qty_max_3_prior_share_of_sku_id,
    sku_id_min_warehouse_capacity_tercile,
    prev_moved_at_hour_percent_rank
FROM metrics
