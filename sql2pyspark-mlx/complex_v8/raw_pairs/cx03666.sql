WITH filtered_moves AS (
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
    WHERE (qty BETWEEN 139 AND 468 OR HOUR(moved_at) BETWEEN 6 AND 12)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(move_type)) AS move_type_clean,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day,
        ABS(unit_price - 299.1) AS unit_price_distance
    FROM filtered_moves
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_clean,
        s.moved_at_day,
        s.unit_price_distance,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        w.capacity AS warehouse_capacity
    FROM derived s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator IN ('op_dew', 'op_chan')
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN sku_category IN ('Beauty', 'Electronics') THEN 'group_a' WHEN sku_category = 'Apparel' THEN 'group_b' ELSE 'other' END AS sku_category_group,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM with_activity
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator, sku_category)) AS distinct_sku_id_in_operator_sku_category,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type, sku_category ORDER BY sku_weight_kg), 3) AS sku_weight_kg_percent_rank
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, distinct_sku_id_in_operator_sku_category, warehouse_capacity
    FROM windowed
) src
UNPIVOT (
    reading FOR field IN (distinct_sku_id_in_operator_sku_category AS m_distinct_sku_id_in_operator_sku_category, warehouse_capacity AS warehouse_capacity_v)
) unp
