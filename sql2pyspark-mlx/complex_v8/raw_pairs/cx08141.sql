WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.opened_on AS warehouse_opened_on
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity > 2614
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_operator_warehouse_id_by_war AS warehouse_opened_on_seq,
        LAG(warehouse_opened_on) OVER w_warehouse_opened_on_year_ope AS prev_warehouse_opened_on,
        LAG(moved_at) OVER (PARTITION BY YEAR(warehouse_opened_on), operator ORDER BY warehouse_opened_on, id) AS prev_moved_at,
        DENSE_RANK() OVER w_warehouse_opened_on_year_ope_2 AS warehouse_opened_on_dense_rank,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SUM(qty) OVER (PARTITION BY operator ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id) AS distinct_warehouse_id_in_sku_id,
        SUM(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_sum_2_prior,
        ROUND(AVG(qty) OVER (ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY MONTH(warehouse_opened_on)), 0), 4) AS qty_share_of_warehouse_opened_on_month,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id), 2) AS sku_id_avg_qty,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS moved_at_dense_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY MONTH(moved_at), operator), 2) AS unit_price_vs_moved_at_month_operator_avg,
        ROUND(AVG(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior,
        COUNT(*) OVER (PARTITION BY MONTH(warehouse_opened_on)) AS rows_in_warehouse_opened_on_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, warehouse_id ORDER BY warehouse_opened_on, id), 3) AS warehouse_opened_on_percent_rank
    FROM combined_details
    WINDOW w_operator_warehouse_id_by_war AS (PARTITION BY operator, warehouse_id ORDER BY warehouse_opened_on, id), w_warehouse_opened_on_year_ope AS (PARTITION BY YEAR(warehouse_opened_on), operator ORDER BY warehouse_opened_on, id), w_warehouse_opened_on_year_ope_2 AS (PARTITION BY YEAR(warehouse_opened_on), operator ORDER BY warehouse_opened_on), w_sku_id AS (PARTITION BY sku_id)
)
SELECT
    id,
    warehouse_id,
    qty,
    warehouse_opened_on_seq,
    prev_warehouse_opened_on,
    qty_sum_2_prior,
    unit_price_sum_2_prior,
    sku_id_avg_qty,
    unit_price_vs_moved_at_month_operator_avg,
    qty_avg_6_prior,
    rows_in_warehouse_opened_on_month,
    warehouse_opened_on_percent_rank
FROM windowed
ORDER BY rows_in_warehouse_opened_on_month NULLS LAST, unit_price_vs_moved_at_month_operator_avg NULLS LAST
