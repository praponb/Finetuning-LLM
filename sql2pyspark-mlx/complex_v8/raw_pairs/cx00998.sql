WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE operator <> 'op_eve'
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM scoped_moves
    WHERE unit_price <= 361.9
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM scored s
    JOIN skus sk ON sk.id = s.sku_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN unit_price_filled >= 375.2 THEN 'HIGH' WHEN unit_price_filled >= 175.7 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_filled_tier,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m,
        UPPER(TRIM(move_type)) AS move_type_clean
    FROM combined_details_sku
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        unit_price,
        unit_price_filled,
        sku_weight_kg,
        sku_is_active,
        moved_at_plus_30m,
        ROUND(AVG(unit_price_filled) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price_filled,
        MAX(unit_price_filled) OVER (ORDER BY sku_weight_kg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_filled_so_far,
        FIRST_VALUE(unit_price) OVER (ORDER BY sku_weight_kg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        MIN(unit_price) OVER (PARTITION BY move_type) AS move_type_min_unit_price,
        LEAD(moved_at_plus_30m) OVER (ORDER BY sku_weight_kg ASC, id) AS next_moved_at_plus_30m,
        NTILE(4) OVER w_all_by_sku_weight_kg AS sku_weight_kg_quartile
    FROM tagged
    WINDOW w_all_by_sku_weight_kg AS (ORDER BY sku_weight_kg ASC, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    unit_price_filled,
    sku_weight_kg,
    moved_at_plus_30m,
    move_type_avg_unit_price_filled,
    max_unit_price_filled_so_far,
    next_moved_at_plus_30m,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    FLOOR(sku_weight_kg / 5) * 5 AS sku_weight_kg_bucket
FROM metrics
ORDER BY move_type_slug, max_unit_price_filled_so_far DESC
