WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CASE WHEN unit_price >= 367.7 THEN 'HIGH' WHEN unit_price >= 215.4 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        CONCAT_WS('-', move_type, operator) AS move_type_operator_label,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE qty BETWEEN 90 AND 387
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM recent_moves
    WHERE unit_price < (SELECT MAX(unit_price) FROM recent_moves)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    unit_price_tier,
    operator_slug,
    LENGTH(TRIM(unit_price_tier)) AS unit_price_tier_len
FROM without_matches
ORDER BY warehouse_id DESC, operator_slug, id
LIMIT 10
