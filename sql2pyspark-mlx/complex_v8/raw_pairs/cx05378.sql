WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price <= 329.7
),
latest_per_key AS (
    -- Keep the latest row per move_type, operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, operator ORDER BY qty DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN qty >= 249 THEN 'HIGH' WHEN qty >= 145 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        ABS(unit_price - 200.7) AS unit_price_distance
    FROM latest_per_key
    WHERE move_type NOT IN ('ADJUST', 'OUTBOUND')
)
SELECT *
FROM (
    SELECT move_type, operator, qty
    FROM scored
) src
PIVOT (
    MAX(qty)
    FOR operator IN ('op_chan', 'op_bee', 'op_eve', 'op_dew')
)
ORDER BY move_type
