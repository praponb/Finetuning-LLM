WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM base_moves
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty <= 445
        )
      AND s.operator <> 'op_ann'
),
latest AS (
    -- Keep the latest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        least_qty_unit_price,
        greatest_unit_price_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY greatest_unit_price_qty DESC, id DESC) AS recency_rank
        FROM screened
    ) ranked
    WHERE recency_rank = 1
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(moved_at) AS first_moved_at,
        MIN(moved_at) AS first_moved_at_2
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        st.first_moved_at_2
    FROM latest s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    least_qty_unit_price,
    first_moved_at,
    ROUND((UNIX_TIMESTAMP(first_moved_at_2) - UNIX_TIMESTAMP(moved_at)) / 60, 1) AS minutes_moved_at_to_first_moved_at_2
FROM with_counts
WHERE least_qty_unit_price BETWEEN 175.7 AND 450.1
ORDER BY first_moved_at
