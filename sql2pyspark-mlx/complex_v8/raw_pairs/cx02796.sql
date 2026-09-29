WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND warehouse_id >= 5
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        days_since_moved_at,
        qty_minus_unit_price,
        MIN(unit_price) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_min_unit_price,
        ROUND(AVG(unit_price) OVER (ORDER BY qty_minus_unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        DENSE_RANK() OVER (ORDER BY qty_minus_unit_price, id) AS qty_minus_unit_price_dense_rank,
        NTH_VALUE(days_since_moved_at, 2) OVER (ORDER BY qty_minus_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at
    FROM scoped
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at_2,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at_3,
        CASE WHEN days_since_moved_at >= 595 THEN 'A' WHEN days_since_moved_at >= 175 THEN 'B' ELSE 'C' END AS days_since_moved_at_tier
    FROM metrics
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        days_since_moved_at,
        qty_minus_unit_price,
        sku_id_warehouse_id_min_unit_price,
        unit_price_avg_4,
        second_days_since_moved_at,
        days_since_moved_at_2,
        days_since_moved_at_3,
        days_since_moved_at_tier
    FROM scored
    WHERE qty_minus_unit_price_dense_rank <= 3
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.qty_minus_unit_price,
        s.sku_id_warehouse_id_min_unit_price,
        s.unit_price_avg_4,
        s.days_since_moved_at_2,
        s.days_since_moved_at_3,
        s.days_since_moved_at_tier,
        sk.category AS sku_category,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM leaders s
    JOIN skus sk ON sk.id = s.sku_id
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.qty_minus_unit_price,
        s.days_since_moved_at_2,
        s.days_since_moved_at_tier,
        s.sku_unit_cost,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type
    FROM with_parent s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric, reading
FROM (
    SELECT id, move_id, qty, move_qty
    FROM detail_rows_moves
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (qty AS qty_v, move_qty AS m_move_qty)
) unp
