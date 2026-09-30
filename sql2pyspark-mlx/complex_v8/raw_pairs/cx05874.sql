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
        CASE WHEN move_type IN ('INBOUND', 'TRANSFER') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
        LENGTH(TRIM(move_type)) AS move_type_len,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty <= 245
      AND unit_price BETWEEN 152.7 AND 323.9
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_group,
        s.unit_price_adj,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM valid_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.moved_at < TIMESTAMP '2025-02-12 13:00:00'
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        sku_id,
        operator,
        move_type_group,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS cnt
    FROM with_activity
    GROUP BY sku_id, operator, move_type_group
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        sku_id,
        operator,
        move_type_group,
        last_moved_at,
        cnt,
        MAX(cnt) OVER w_operator AS operator_max_cnt,
        RANK() OVER (PARTITION BY operator ORDER BY cnt DESC) AS cnt_rank,
        LAG(last_moved_at) OVER (PARTITION BY operator ORDER BY cnt DESC, sku_id, operator, move_type_group) AS prev_last_moved_at,
        SUM(cnt) OVER (PARTITION BY operator ORDER BY cnt DESC, sku_id, operator, move_type_group ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cnt,
        NTH_VALUE(last_moved_at, 2) OVER (PARTITION BY operator ORDER BY cnt DESC, sku_id, operator, move_type_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_moved_at
    FROM grouped
    WINDOW w_operator AS (PARTITION BY operator)
)
-- Native SQL UNPIVOT (wide to long)
SELECT sku_id, operator, field, value
FROM (
    SELECT sku_id, operator, cnt_rank, operator_max_cnt, running_cnt, cnt
    FROM metrics
) src
UNPIVOT (
    value FOR field IN (cnt_rank, operator_max_cnt, running_cnt, cnt)
) unp
