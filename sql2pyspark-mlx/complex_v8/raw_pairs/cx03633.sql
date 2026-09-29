WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
)
SELECT
    operator AS label,
    unit_price,
    moved_at,
    'move' AS origin
FROM valid_moves
WHERE operator IN ('op_eve', 'op_dew', 'op_bee')
  AND unit_price > 125.9
UNION ALL
SELECT
    operator AS label,
    unit_price,
    moved_at,
    'move_2' AS origin
FROM stock_moves
ORDER BY label
