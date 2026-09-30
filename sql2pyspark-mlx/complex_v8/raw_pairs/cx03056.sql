WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price <= 396.4
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM prepared s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(moved_at) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        LAG(sku_unit_cost, 3) OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id) AS prev3_sku_unit_cost,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER (PARTITION BY sku_id, sku_category), 2) AS sku_unit_cost_vs_sku_id_sku_category_avg,
        ROW_NUMBER() OVER w_warehouse_id_move_type_by_mo_2 AS moved_at_seq,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id, sku_category)) AS distinct_move_type_in_sku_id_sku_category,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, TO_DATE(moved_at) ORDER BY qty, id) AS qty_dense_rank,
        MIN(sku_unit_cost) OVER (PARTITION BY sku_category, DATE_TRUNC('MONTH', moved_at)) AS sku_category_moved_at_month_min_sku_unit_cost,
        DENSE_RANK() OVER (PARTITION BY move_type, sku_category ORDER BY moved_at, id) AS moved_at_dense_rank
    FROM with_details
    WINDOW w_warehouse_id_move_type_by_mo_2 AS (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        sku_code,
        sku_unit_cost,
        sku_category,
        first_moved_at,
        prev3_sku_unit_cost,
        sku_unit_cost_vs_sku_id_sku_category_avg,
        distinct_move_type_in_sku_id_sku_category,
        qty_dense_rank,
        sku_category_moved_at_month_min_sku_unit_cost,
        moved_at_dense_rank
    FROM metrics
    WHERE moved_at_seq <= 3
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    sku_code,
    sku_unit_cost,
    first_moved_at,
    prev3_sku_unit_cost,
    qty_dense_rank,
    moved_at_dense_rank,
    ABS(qty - 239) AS qty_distance
FROM top_per_group
WHERE warehouse_id IS NOT NULL
ORDER BY id DESC, moved_at_dense_rank DESC NULLS FIRST
