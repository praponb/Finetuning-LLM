WITH scoped AS (
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
    WHERE sku_id IS NOT NULL
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM scoped s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.*,
        sk.weight_kg AS sku_weight_kg_2,
        sk.unit_cost AS sku_unit_cost_2,
        sk.category AS sku_category_2
    FROM joined_sku s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.weight_kg <= 20.9
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        sku_weight_kg,
        sku_category,
        sku_category_2,
        ABS(sku_weight_kg - 12.4) AS sku_weight_kg_distance,
        CASE WHEN sku_unit_cost_2 >= 251.2 THEN 'A' WHEN sku_unit_cost_2 >= 108.5 THEN 'B' ELSE 'C' END AS sku_unit_cost_2_tier
    FROM enriched_sku
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sku_unit_cost_2_tier ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(sku_weight_kg_distance) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_sku_weight_kg_distance,
        ROUND(qty - AVG(qty) OVER w_warehouse_id, 2) AS qty_vs_warehouse_id_avg,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id) AS seq_no,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER (PARTITION BY warehouse_id), 0), 4) AS sku_weight_kg_share_of_warehouse_id
    FROM enriched
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    COALESCE(sku_unit_cost_2_tier, 'ALL') AS sku_unit_cost_2_tier,
    seq_no,
    SUM(CASE WHEN sku_category_2 IN ('Beauty', 'Electronics', 'Apparel') THEN 1 ELSE 0 END) AS n_sku_category_2_in_beauty_ele,
    COUNT(*) AS cnt
FROM ranked
GROUP BY ROLLUP(warehouse_id, sku_unit_cost_2_tier, seq_no)
ORDER BY warehouse_id NULLS LAST, sku_unit_cost_2_tier NULLS LAST, seq_no NULLS LAST
