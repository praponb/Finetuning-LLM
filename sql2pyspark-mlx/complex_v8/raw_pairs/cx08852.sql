WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR warehouse_id IS NOT NULL)
),
stats AS (
    -- Summarise per group
    SELECT
        sku_id,
        operator,
        MIN(unit_price) AS min_unit_price,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        MAX(unit_price) AS max_unit_price,
        COUNT(*) FILTER (WHERE move_type = 'INBOUND') AS n_move_type_inbound
    FROM valid_moves
    GROUP BY 1, 2
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.sku_id,
        s.operator,
        s.min_unit_price,
        s.max_unit_price,
        s.n_move_type_inbound,
        sk.labels AS sku_labels
    FROM stats s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.sku_id,
        s.operator,
        s.min_unit_price,
        s.n_move_type_inbound,
        s.sku_labels,
        sk.sku_code,
        sk.is_active AS sku_is_active
    FROM with_parent_sku s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.is_active = TRUE
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_details_sku s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price <= 337.6
        )
)
SELECT
    sku_id,
    operator,
    min_unit_price,
    n_move_type_inbound,
    sku_code
FROM with_activity
ORDER BY sku_code NULLS LAST, n_move_type_inbound NULLS LAST
