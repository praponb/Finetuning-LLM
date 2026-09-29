WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
latest AS (
    -- Keep the latest row per warehouse_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM latest
    WHERE sku_id <= 12
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code
    FROM derived s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    sku_id,
    move_type,
    GROUPING_ID() AS gid,
    ROUND(SUM(sku_weight_kg), 2) AS total_sku_weight_kg,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM with_details
GROUP BY GROUPING SETS ((sku_id, move_type), (sku_id), (move_type), ())
