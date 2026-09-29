WITH filtered AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE qty <= 256
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE unit_price > (SELECT AVG(unit_price) FROM filtered)
),
without_matches_2 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 3
              AND st.qty BETWEEN 78 AND 394
        )
      AND s.qty >= 169
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels
    FROM without_matches_2 s
    JOIN skus sk ON sk.id = s.sku_id
),
with_parent_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.sku_unit_cost,
        s.sku_is_active,
        sk.labels AS sku_labels_2
    FROM with_parent s
    JOIN skus sk ON sk.id = s.sku_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        sku_id,
        sku_is_active,
        ROUND(STDDEV(sku_unit_cost), 2) AS stddev_sku_unit_cost,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        MAX(unit_price) AS max_unit_price,
        COUNT(*) AS row_count
    FROM with_parent_sku
    GROUP BY 1, 2, 3
),
joined AS (
    -- Join skus
    SELECT
        s.warehouse_id,
        s.sku_id,
        s.sku_is_active,
        s.stddev_sku_unit_cost,
        s.max_unit_price,
        s.row_count,
        sk.is_active AS sku_is_active_2,
        sk.sku_code,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM stats s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT warehouse_id, sku_id, measure, value
FROM (
    SELECT warehouse_id, sku_id, max_unit_price, stddev_sku_unit_cost
    FROM joined
) src
UNPIVOT (
    value FOR measure IN (max_unit_price, stddev_sku_unit_cost)
) unp
ORDER BY warehouse_id, sku_id, measure
