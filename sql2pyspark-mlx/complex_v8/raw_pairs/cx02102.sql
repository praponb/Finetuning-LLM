WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.qty) AS prior_qty_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_1d,
        sk.is_active AS sku_is_active
    FROM in_window s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.is_active = FALSE
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_1d,
        s.sku_is_active,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM with_details s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
tagged AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(move_operator), 1, 4) AS move_operator_prefix
    FROM detail_rows
    WHERE move_unit_price BETWEEN 112.8 AND 367.6
)
SELECT *
FROM (
    SELECT prior_rows_1d, move_operator, move_unit_price
    FROM tagged
) src
PIVOT (
    MAX(move_unit_price)
    FOR move_operator IN ('op_eve', 'op_dew', 'op_ann')
)
ORDER BY prior_rows_1d
