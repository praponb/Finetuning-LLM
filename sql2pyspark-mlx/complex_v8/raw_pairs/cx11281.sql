WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE move_type <> 'INBOUND'
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
without_matches AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND SIZE(sk.labels) >= 1
              AND sk.unit_cost < 320.4
        )
)
SELECT *
FROM (
    SELECT prev_moved_at, sku_id, operator, unit_price
    FROM without_matches
) src
PIVOT (
    SUM(unit_price)
    FOR operator IN ('op_dew', 'op_eve')
)
ORDER BY prev_moved_at, sku_id
