WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(move_type_prefix), 1, 3) AS move_type_prefix_prefix,
        LENGTH(TRIM(move_type_prefix)) AS move_type_prefix_len
    FROM cleaned_moves
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type IN ('TRANSFER', 'ADJUST')
              AND st.warehouse_id IS NOT NULL
        )
),
qualified AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND UPPER(TRIM(sk.sku_code)) LIKE 'A%'
        )
)
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    move_type_prefix,
    move_type_prefix_prefix,
    move_type_prefix_len,
    CASE WHEN operator IN ('op_bee', 'op_ann') THEN 'group_a' WHEN operator = 'op_eve' THEN 'group_b' ELSE 'other' END AS operator_group,
    LEAST(move_type_prefix_len, qty) AS least_move_type_prefix_len_qty
FROM qualified
ORDER BY qty
