WITH recent AS (
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
    WHERE warehouse_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_id, 0), 4) AS unit_price_share_of_warehouse_id,
        DENSE_RANK() OVER w_operator_by_qty_ties AS qty_dense_rank,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        NTILE(4) OVER (PARTITION BY operator ORDER BY moved_at, id) AS moved_at_quartile,
        LAG(moved_at, 2) OVER w_operator_by_qty AS prev2_moved_at,
        ROUND(qty - AVG(qty) OVER w_warehouse_id, 2) AS qty_vs_warehouse_id_avg,
        NTH_VALUE(qty, 2) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price
    FROM recent
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_operator_by_qty_ties AS (PARTITION BY operator ORDER BY qty ASC), w_operator_by_qty AS (PARTITION BY operator ORDER BY qty ASC, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE qty_dense_rank <= 3
      AND qty_vs_warehouse_id_avg >= -200.0
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    unit_price,
    unit_price_share_of_warehouse_id,
    qty_dense_rank,
    second_moved_at,
    moved_at_quartile,
    prev2_moved_at,
    qty_vs_warehouse_id_avg,
    second_qty,
    rows_in_warehouse_id,
    warehouse_id_max_unit_price,
    CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM top_per_group
WHERE second_qty > 109
ORDER BY moved_at_quartile DESC NULLS FIRST, move_type_group, id
LIMIT 5
