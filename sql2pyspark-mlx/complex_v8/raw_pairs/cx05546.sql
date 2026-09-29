WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label,
        CASE WHEN unit_price >= 363.6 THEN 'large' WHEN unit_price >= 152.7 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'ADJUST')
      AND qty <= 348
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(unit_price_tier), 1, 4) AS unit_price_tier_prefix,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        CONCAT_WS(': ', unit_price_tier, move_type_operator_label) AS unit_price_tier_move_type_operator_label_label
    FROM valid
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT s.*, sk.category AS sku_category, sk.is_active AS sku_is_active
    FROM scored s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    days_since_moved_at,
    move_type_operator_label,
    unit_price_tier_prefix,
    unit_price_adj,
    sku_category,
    SUBSTRING(TRIM(sku_category), 1, 4) AS sku_category_prefix
FROM with_details_sku
WHERE qty >= 148
ORDER BY sku_category DESC, days_since_moved_at DESC
