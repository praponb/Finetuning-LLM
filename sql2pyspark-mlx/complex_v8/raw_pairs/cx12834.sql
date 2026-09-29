WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        qty + unit_price AS qty_plus_unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.qty_plus_unit_price,
        sk.unit_cost AS sku_unit_cost
    FROM prepared s
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty,
        ROUND(CUME_DIST() OVER w_warehouse_id_operator_by_sku, 3) AS sku_unit_cost_cume_dist,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        MAX(qty_plus_unit_price) OVER (PARTITION BY warehouse_id, operator ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_plus_unit_price_max_4,
        NTH_VALUE(qty_plus_unit_price, 2) OVER (PARTITION BY warehouse_id, operator ORDER BY sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_plus_unit_price,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY warehouse_id, operator ORDER BY sku_unit_cost DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_6_prior,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id
    FROM with_details_sku
    WINDOW w_warehouse_id_operator_by_sku AS (PARTITION BY warehouse_id, operator ORDER BY sku_unit_cost DESC)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(rows_in_sku_id, 0) AS rows_in_sku_id_filled,
        sku_unit_cost_cume_dist + qty AS sku_unit_cost_cume_dist_plus_qty,
        CASE WHEN rows_in_sku_id >= 13 THEN 'HIGH' WHEN rows_in_sku_id >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_sku_id_tier
    FROM windowed
    WHERE sku_unit_cost_avg_6_prior < 220.4
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, rows_in_sku_id_tier, rows_in_sku_id
    FROM labeled
) src
PIVOT (
    SUM(rows_in_sku_id)
    FOR rows_in_sku_id_tier IN ('HIGH', 'LOW')
)
