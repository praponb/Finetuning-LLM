WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE warehouse_id >= 5
),
with_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_operator_label,
        s.operator_ref,
        sk.unit_cost AS sku_unit_cost,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM filtered_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.unit_cost BETWEEN 17.3 AND 330.6
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_sum_unit_price,
        NTILE(10) OVER w_warehouse_id_sku_id_by_moved AS moved_at_decile,
        COUNT(*) OVER w_sku_id_warehouse_id AS rows_in_sku_id_warehouse_id,
        LEAD(sku_unit_cost, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), MONTH(warehouse_opened_on) ORDER BY sku_unit_cost DESC, id) AS next_sku_unit_cost,
        SUM(sku_unit_cost) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), MONTH(warehouse_opened_on) ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sku_unit_cost_sum_3_prior,
        LEAD(moved_at) OVER w_operator_by_warehouse_opened AS next_moved_at,
        LAST_VALUE(qty) OVER (PARTITION BY operator ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, warehouse_id), 2) AS qty_vs_sku_id_warehouse_id_avg,
        LEAD(unit_price) OVER w_moved_at_month_warehouse_ope AS next_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), MONTH(warehouse_opened_on) ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior
    FROM with_details
    WINDOW w_warehouse_id_sku_id_by_moved AS (PARTITION BY warehouse_id, sku_id ORDER BY moved_at DESC, id), w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_operator_by_warehouse_opened AS (PARTITION BY operator ORDER BY warehouse_opened_on, id), w_moved_at_month_warehouse_ope AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), MONTH(warehouse_opened_on) ORDER BY sku_unit_cost DESC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        moved_at,
        unit_price,
        operator_ref,
        sku_unit_cost,
        warehouse_wh_code,
        warehouse_opened_on,
        sku_id_warehouse_id_sum_unit_price,
        moved_at_decile,
        rows_in_sku_id_warehouse_id,
        sku_unit_cost_sum_3_prior,
        next_moved_at,
        last_qty,
        qty_vs_sku_id_warehouse_id_avg,
        next_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_id ORDER BY sku_id_warehouse_id_sum_unit_price DESC), 3) AS sku_id_warehouse_id_sum_unit_price_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY last_qty ASC, id) AS last_qty_seq,
        MIN(unit_price) OVER w_operator AS operator_min_unit_price,
        MAX(sku_unit_cost) OVER (PARTITION BY operator) AS operator_max_sku_unit_cost,
        NTILE(5) OVER w_sku_id_by_sku_id_warehouse_i_2 AS sku_id_warehouse_id_sum_unit_price_quintile,
        ROUND(next_unit_price - AVG(next_unit_price) OVER w_operator, 2) AS next_unit_price_vs_operator_avg,
        MAX(qty_avg_1_prior) OVER (PARTITION BY operator) AS operator_max_qty_avg_1_prior,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator
    FROM ranked
    WINDOW w_operator AS (PARTITION BY operator), w_sku_id_by_sku_id_warehouse_i_2 AS (PARTITION BY sku_id ORDER BY sku_id_warehouse_id_sum_unit_price DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        SUM(unit_price) AS total_unit_price,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(warehouse_wh_code)) >= 5 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_warehouse_wh_cod
    FROM windowed
    GROUP BY 1
)
SELECT
    moved_at_month,
    pct_length_trim_warehouse_wh_cod,
    DATEDIFF(DATE '2025-06-30', moved_at_month) AS days_since_moved_at_month
FROM grouped
WHERE moved_at_month < TIMESTAMP '2025-02-09 09:00:00'
ORDER BY days_since_moved_at_month
