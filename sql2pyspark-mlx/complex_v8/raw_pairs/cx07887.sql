WITH valid_moves AS (
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
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
filtered AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM valid_moves s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND UPPER(TRIM(sk.sku_code)) LIKE 'A%'
              AND NOT ARRAY_CONTAINS(sk.labels, 'express')
        )
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE unit_price < (SELECT MAX(unit_price) FROM filtered)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN unit_price >= 323.9 THEN 'HIGH' WHEN unit_price >= 160.8 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        operator,
        sku_id,
        COUNT(*) FILTER (WHERE operator IN ('op_ann', 'op_bee', 'op_chan')) AS n_operator_in_op_ann_op_bee_op,
        SUM(CASE WHEN operator IN ('op_chan', 'op_dew', 'op_bee') THEN 1 ELSE 0 END) AS n_operator_in_op_chan_op_dew_o
    FROM without_matches
    GROUP BY CASE WHEN unit_price >= 323.9 THEN 'HIGH' WHEN unit_price >= 160.8 THEN 'MEDIUM' ELSE 'LOW' END, operator, sku_id
)
SELECT
    unit_price_tier,
    operator,
    n_operator_in_op_ann_op_bee_op,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
    CASE WHEN unit_price_tier IN ('LOW', 'MEDIUM') THEN 'group_a' WHEN unit_price_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS unit_price_tier_group
FROM grouped
WHERE operator = 'op_bee'
ORDER BY operator DESC
