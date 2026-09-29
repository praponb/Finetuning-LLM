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
        qty - unit_price AS qty_minus_unit_price,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 12
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_minus_unit_price,
        s.greatest_qty_unit_price,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM joined s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY unit_price ASC, id) AS unit_price_dense_rank,
        COUNT(*) OVER w_warehouse_city AS rows_in_warehouse_city
    FROM summarized_rows
    WINDOW w_warehouse_city AS (PARTITION BY warehouse_city)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(unit_price) OVER (ORDER BY first_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        NTH_VALUE(first_moved_at, 2) OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_moved_at,
        ROUND(AVG(warehouse_capacity) OVER (ORDER BY first_moved_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_3,
        LAST_VALUE(first_moved_at) OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_moved_at,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, operator)) AS distinct_move_type_in_warehouse_id_operator
    FROM sequenced
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.qty_minus_unit_price,
        s.greatest_qty_unit_price,
        s.warehouse_capacity,
        s.warehouse_city,
        s.first_moved_at,
        s.last_unit_price,
        s.second_first_moved_at,
        s.last_first_moved_at,
        sk.is_active AS sku_is_active,
        sk.sku_code
    FROM metrics s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    qty_minus_unit_price,
    greatest_qty_unit_price,
    warehouse_capacity,
    last_unit_price,
    second_first_moved_at,
    sku_is_active,
    sku_code,
    IF(sku_is_active, 'yes', 'no') AS sku_is_active_label,
    CASE WHEN operator IN ('op_chan', 'op_ann') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
FROM enriched_sku
ORDER BY warehouse_capacity DESC
