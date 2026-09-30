WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE sku_id <= 11
      AND qty > 78
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        unit_price_bucket,
        operator_clean,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, operator), 2) AS warehouse_id_operator_avg_qty,
        MAX(unit_price_bucket) OVER (PARTITION BY move_type_or_unknown ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_bucket_so_far,
        SUM(unit_price) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_sum_unit_price,
        NTH_VALUE(unit_price_bucket, 2) OVER (PARTITION BY move_type_or_unknown ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_bucket,
        MIN(qty) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_min_qty
    FROM filtered
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.unit_price_bucket,
        s.operator_clean,
        s.max_unit_price_bucket_so_far,
        s.warehouse_id_operator_min_qty,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM sequenced s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    max_unit_price_bucket_so_far,
    warehouse_id_operator_min_qty,
    warehouse_wh_code,
    warehouse_opened_on
FROM combined_details
ORDER BY unit_price DESC NULLS FIRST
