WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE operator IN ('op_bee', 'op_ann')
      AND move_type NOT IN ('OUTBOUND', 'TRANSFER')
),
latest_per_key AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id DESC) AS recency_rank
        FROM prepared
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, field, amount
FROM (
    SELECT id, unit_price_filled, unit_price
    FROM latest_per_key
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (unit_price_filled, unit_price)
) unp
