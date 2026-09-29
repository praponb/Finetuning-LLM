WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN qty >= 372 THEN 'A' WHEN qty >= 111 THEN 'B' ELSE 'C' END AS qty_tier,
        LENGTH(TRIM(move_type)) AS move_type_len,
        CASE WHEN qty >= 415 THEN 'large' WHEN qty >= 189 THEN 'medium' ELSE 'small' END AS qty_tier_2
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND moved_at >= TIMESTAMP '2025-01-19 12:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        qty_tier,
        move_type_len,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        CASE WHEN unit_price >= 290.4 THEN 'HIGH' WHEN unit_price >= 236.5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter
    FROM cleaned
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(qty / NULLIF(unit_price_adj, 0), 3) AS qty_per_unit_price_adj,
        ROUND(move_type_len / NULLIF(qty, 0), 3) AS move_type_len_per_qty
    FROM tagged
),
scored AS (
    -- Add calculated columns
    SELECT *, FLOOR(move_type_len / 2) * 2 AS move_type_len_bucket
    FROM annotated
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    qty_tier,
    move_type_len,
    unit_price_adj,
    unit_price_tier,
    move_type_len_per_qty,
    move_type_len_bucket,
    LEAST(unit_price_adj, qty) AS least_unit_price_adj_qty
FROM scored
ORDER BY move_type, move_type_len NULLS LAST
