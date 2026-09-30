WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 290.4 THEN 'A' WHEN unit_price >= 200.7 THEN 'B' ELSE 'C' END AS unit_price_tier,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE warehouse_id >= 3
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        unit_price_tier,
        move_type_slug,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        CONCAT_WS('|', move_type, move_type_slug) AS move_type_move_type_slug_label,
        COALESCE(qty, 0) AS qty_filled
    FROM valid_moves
    WHERE qty >= 85
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.unit_price_tier,
        s.qty_filled,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.warehouse_id AS move_warehouse_id
    FROM enriched s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.unit_price_tier,
        s.move_id,
        s.move_move_type,
        s.move_warehouse_id,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg,
        w.wh_code AS warehouse_wh_code
    FROM exploded_detail s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.move_warehouse_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(unit_price_tier, '#', CAST(move_id AS STRING)) AS unit_price_tier_ref,
        SUBSTRING(TRIM(move_move_type), 1, 2) AS move_move_type_prefix
    FROM with_details
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        unit_price_tier,
        move_id,
        move_move_type,
        sku_is_active,
        sku_weight_kg,
        warehouse_wh_code,
        unit_price_tier_ref,
        move_move_type_prefix,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY move_id, move_warehouse_id ORDER BY qty DESC NULLS FIRST, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        ROW_NUMBER() OVER (PARTITION BY move_id ORDER BY sku_weight_kg ASC, id, move_id) AS seq_no,
        MAX(qty) OVER (PARTITION BY move_id, move_warehouse_id ORDER BY qty DESC NULLS FIRST, id, move_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_max_1_prior,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_id, move_warehouse_id ORDER BY qty DESC NULLS FIRST, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        COUNT(*) OVER (PARTITION BY move_id, warehouse_id ORDER BY sku_weight_kg DESC NULLS FIRST, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM scored
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        unit_price_tier,
        move_id,
        move_move_type,
        sku_is_active,
        unit_price_tier_ref,
        move_move_type_prefix,
        seq_no,
        qty_max_1_prior,
        seq_no - qty_max_1_prior AS seq_no_minus_qty_max_1_prior
    FROM with_trends
    WHERE warehouse_id IS NOT NULL
)
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price_tier,
    move_move_type,
    sku_is_active,
    move_move_type_prefix,
    qty_max_1_prior,
    LENGTH(TRIM(move_move_type)) AS move_move_type_len
FROM tagged
ORDER BY sku_id, unit_price_tier NULLS LAST
