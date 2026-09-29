WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref_2,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE unit_price > 95.8
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type IS NOT NULL
              AND st.sku_id IS NOT NULL
        )
      AND s.qty < 355
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id)) AS distinct_move_type_in_sku_id,
        ROUND(PERCENT_RANK() OVER w_sku_id_warehouse_id_by_unit, 3) AS unit_price_per_qty_percent_rank,
        DENSE_RANK() OVER (PARTITION BY sku_id, move_type ORDER BY unit_price) AS unit_price_dense_rank,
        MAX(unit_price) OVER (PARTITION BY sku_id) AS sku_id_max_unit_price,
        ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY unit_price, id) AS unit_price_seq,
        MIN(unit_price) OVER (PARTITION BY sku_id) AS sku_id_min_unit_price
    FROM without_matches
    WINDOW w_sku_id_warehouse_id_by_unit AS (PARTITION BY sku_id, warehouse_id ORDER BY unit_price_per_qty)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, ROUND(AVG(qty), 2) AS avg_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty
    FROM metrics s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN avg_move_qty >= 400.2 THEN 'HIGH' WHEN avg_move_qty >= 200.6 THEN 'MEDIUM' ELSE 'LOW' END AS avg_move_qty_tier
    FROM summarized_rows
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.move_type_ref,
        s.unit_price_per_qty,
        s.distinct_move_type_in_sku_id,
        s.unit_price_per_qty_percent_rank,
        s.unit_price_dense_rank,
        s.sku_id_max_unit_price,
        s.unit_price_seq,
        s.sku_id_min_unit_price,
        s.avg_move_qty,
        s.avg_move_qty_tier,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        sk.sku_code
    FROM labeled s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, unit_price_seq, qty, distinct_move_type_in_sku_id, unit_price_dense_rank
    FROM enriched_warehouse
) src
UNPIVOT (
    value FOR field IN (unit_price_seq AS unit_price_seq_v, qty AS m_qty, distinct_move_type_in_sku_id AS distinct_move_type_in_sku_id_v, unit_price_dense_rank AS m_unit_price_dense_rank)
) unp
ORDER BY id, field
