WITH recent AS (
    -- Scope the stock_moves rows
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
    WHERE operator NOT IN ('op_eve', 'op_bee')
      AND MONTH(moved_at) IN (1, 3)
),
combined_details AS (
    -- Attach sku attributes
    SELECT s.*, sk.category AS sku_category
    FROM recent s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND ARRAY_CONTAINS(sk.labels, 'fragile')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(qty) OVER (PARTITION BY operator, MONTH(moved_at)), 2) AS operator_moved_at_month_avg_qty,
        LAST_VALUE(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price DESC) AS unit_price_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id ORDER BY qty DESC), 3) AS qty_cume_dist,
        SUM(unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_sum_2_prior,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_category ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id,
        MIN(qty) OVER (PARTITION BY sku_id) AS sku_id_min_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, sku_category ORDER BY unit_price DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2,
        LEAD(unit_price) OVER w_warehouse_id_by_moved_at AS next_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, MONTH(moved_at)), 0), 4) AS unit_price_share_of_operator_moved_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST), 3) AS moved_at_percent_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_3
    FROM combined_details
    WINDOW w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at NULLS LAST, id)
)
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    unit_price,
    sku_category,
    last_unit_price,
    qty_cume_dist,
    unit_price_sum_2_prior,
    second_qty,
    qty_share_of_sku_id,
    sku_id_min_qty,
    next_unit_price,
    unit_price_share_of_operator_moved_at_month,
    moved_at_percent_rank
FROM with_windows
ORDER BY next_unit_price DESC
