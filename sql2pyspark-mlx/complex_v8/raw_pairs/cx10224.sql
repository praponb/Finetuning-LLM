WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        CASE WHEN unit_price >= 289.7 THEN 'HIGH' WHEN unit_price >= 205.1 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.move_type_clean,
        s.unit_price_tier,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels
    FROM recent s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
latest_per_key AS (
    -- Keep the earliest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        move_type_clean,
        unit_price_tier,
        warehouse_wh_code,
        warehouse_opened_on,
        warehouse_capacity,
        sku_labels
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY warehouse_capacity, id) AS recency_rank
        FROM with_details_warehouse
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(warehouse_wh_code), ' ', '_')) AS warehouse_wh_code_slug,
        LOWER(REGEXP_REPLACE(TRIM(unit_price_tier), ' ', '_')) AS unit_price_tier_slug,
        DAYOFWEEK(warehouse_opened_on) AS warehouse_opened_on_dayofweek
    FROM latest_per_key
)
SELECT
    move_type,
    operator,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(warehouse_capacity) / NULLIF(SUM(warehouse_opened_on_dayofweek), 0), 3) AS warehouse_capacity_to_warehouse_opened_on_dayofw,
    ROUND(SUM(warehouse_opened_on_dayofweek) / NULLIF(SUM(warehouse_capacity), 0), 3) AS warehouse_opened_on_dayofweek_to_warehouse_capac,
    SUM(warehouse_opened_on_dayofweek) AS total_warehouse_opened_on_dayofweek
FROM enriched
GROUP BY ROLLUP(move_type, operator)
ORDER BY move_type NULLS LAST, operator NULLS LAST
