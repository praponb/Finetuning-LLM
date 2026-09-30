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
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.days_since_moved_at,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM filtered_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.category = 'Toys'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(days_since_moved_at) OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_moved_at_so_far,
        NTH_VALUE(qty, 2) OVER (ORDER BY sku_weight_kg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LAG(sku_weight_kg, 1, 0) OVER w_all_by_sku_weight_kg AS prev_sku_weight_kg,
        ROW_NUMBER() OVER (ORDER BY days_since_moved_at, id) AS days_since_moved_at_seq,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, sku_id), 0), 4) AS qty_share_of_operator_sku_id,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_share_of_warehouse_id,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_category ORDER BY sku_weight_kg ASC NULLS LAST), 3) AS sku_weight_kg_cume_dist,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_category, warehouse_id ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(AVG(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS qty_avg_7,
        ROUND(AVG(qty) OVER (PARTITION BY sku_category ORDER BY sku_weight_kg ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior
    FROM joined_sku
    WINDOW w_all_by_sku_weight_kg AS (ORDER BY sku_weight_kg NULLS LAST, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        move_type,
        moved_at,
        operator,
        days_since_moved_at,
        sku_category,
        second_qty,
        prev_sku_weight_kg,
        days_since_moved_at_seq,
        qty_share_of_operator_sku_id,
        sku_weight_kg_cume_dist,
        last_moved_at,
        qty_avg_7,
        qty_avg_6_prior,
        SUM(qty_avg_7) OVER (PARTITION BY operator ORDER BY qty_share_of_warehouse_id NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_avg_7_sum_2,
        SUM(sku_weight_kg) OVER (PARTITION BY operator ORDER BY moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sku_weight_kg_sum_7,
        NTH_VALUE(qty_share_of_warehouse_id, 2) OVER (PARTITION BY operator ORDER BY qty_share_of_warehouse_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_share_of_warehouse_id
    FROM windowed
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at,
    operator,
    days_since_moved_at,
    sku_category,
    second_qty,
    days_since_moved_at_seq,
    last_moved_at,
    qty_avg_7,
    qty_avg_7_sum_2,
    sku_weight_kg_sum_7,
    second_qty_share_of_warehouse_id
FROM metrics
ORDER BY second_qty DESC, id
LIMIT 5
