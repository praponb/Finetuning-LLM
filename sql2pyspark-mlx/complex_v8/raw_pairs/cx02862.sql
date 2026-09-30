WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LOWER(TRIM(operator)) AS operator_clean,
        unit_price - qty AS unit_price_minus_qty,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE sku_id <= 7
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        unit_price,
        operator_clean,
        unit_price_minus_qty,
        unit_price_bucket,
        CASE WHEN unit_price_minus_qty >= 850.1 THEN 'large' WHEN unit_price_minus_qty >= 400.6 THEN 'medium' ELSE 'small' END AS unit_price_minus_qty_tier,
        UPPER(TRIM(operator)) AS operator_clean_2,
        ROUND(unit_price_minus_qty * 1.07, 2) AS unit_price_minus_qty_adj
    FROM prepared
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        operator,
        unit_price,
        operator_clean,
        unit_price_minus_qty,
        unit_price_bucket,
        unit_price_minus_qty_tier,
        operator_clean_2,
        unit_price_minus_qty_adj,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        UPPER(TRIM(move_type)) AS move_type_clean,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix_2
    FROM tagged
    WHERE unit_price >= 112.8
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        operator,
        unit_price,
        operator_clean,
        unit_price_minus_qty,
        unit_price_bucket,
        operator_clean_2,
        unit_price_minus_qty_adj,
        move_type_prefix,
        move_type_clean,
        move_type_prefix_2,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price_minus_qty_adj DESC, id), 3) AS unit_price_minus_qty_adj_cume_dist,
        ROUND(AVG(unit_price_minus_qty_adj) OVER (PARTITION BY operator, unit_price_minus_qty_tier), 2) AS operator_unit_price_minus_qty_tier_avg_unit_pric,
        RANK() OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price_minus_qty_adj DESC) AS unit_price_minus_qty_adj_rank
    FROM tagged_2
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(unit_price_minus_qty_adj * 0.25, 2) AS unit_price_minus_qty_adj_adj
    FROM metrics
    WHERE LENGTH(TRIM(move_type_clean)) >= 6
)
SELECT id, field, reading
FROM (
    SELECT id, unit_price_bucket, unit_price_minus_qty_adj_rank
    FROM annotated
) src
UNPIVOT (
    reading FOR field IN (unit_price_bucket AS unit_price_bucket_v, unit_price_minus_qty_adj_rank AS m_unit_price_minus_qty_adj_rank)
) unp
ORDER BY id, field
