WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label_2
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND move_type IS NOT NULL
),
stats AS (
    -- Summarise per group
    SELECT
        sku_id,
        ROUND(100 * COUNT(CASE WHEN operator NOT IN ('op_eve', 'op_chan') THEN 1 END) / COUNT(*), 1) AS pct_operator_not_in_op_eve_op_ch,
        COUNT(*) FILTER (WHERE operator IS NOT NULL) AS n_operator_is_not_null
    FROM filtered_moves
    GROUP BY sku_id
)
-- Final projection
SELECT DISTINCT sku_id, pct_operator_not_in_op_eve_op_ch
FROM stats
ORDER BY sku_id DESC NULLS FIRST
