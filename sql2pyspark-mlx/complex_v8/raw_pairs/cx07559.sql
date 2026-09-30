WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE unit_price < 313.8
      AND qty >= 109
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.greatest_qty_unit_price,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.city IN ('Kuala Lumpur', 'Singapore')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        warehouse_capacity,
        COUNT(*) OVER (PARTITION BY warehouse_id, sku_id) AS rows_in_warehouse_id_sku_id,
        FIRST_VALUE(greatest_qty_unit_price) IGNORE NULLS OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_qty_unit_price,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY operator), 2) AS warehouse_capacity_vs_operator_avg,
        SUM(greatest_qty_unit_price) OVER w_operator AS operator_sum_greatest_qty_unit_price,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator,
        ROUND(greatest_qty_unit_price / NULLIF(SUM(greatest_qty_unit_price) OVER w_warehouse_id_sku_id, 0), 4) AS greatest_qty_unit_price_share_of_warehouse_id_sk,
        ROUND(CUME_DIST() OVER w_warehouse_id_operator_by_mov_2, 3) AS moved_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY warehouse_capacity DESC), 3) AS warehouse_capacity_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY warehouse_capacity, id) AS seq_no,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id, operator ORDER BY greatest_qty_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY move_type ORDER BY warehouse_capacity DESC, id))) / 60, 1) AS minutes_since_prev_moved_at
    FROM enriched
    WINDOW w_operator AS (PARTITION BY operator), w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_warehouse_id_operator_by_mov_2 AS (PARTITION BY warehouse_id, operator ORDER BY moved_at NULLS LAST)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        warehouse_capacity,
        rows_in_warehouse_id_sku_id,
        first_known_greatest_qty_unit_price,
        warehouse_capacity_vs_operator_avg,
        operator_sum_greatest_qty_unit_price,
        distinct_sku_id_in_operator,
        greatest_qty_unit_price_share_of_warehouse_id_sk,
        moved_at_cume_dist,
        warehouse_capacity_percent_rank,
        second_moved_at,
        minutes_since_prev_moved_at
    FROM ranked
    WHERE seq_no <= 2
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    warehouse_capacity,
    rows_in_warehouse_id_sku_id,
    first_known_greatest_qty_unit_price,
    warehouse_capacity_vs_operator_avg,
    operator_sum_greatest_qty_unit_price,
    distinct_sku_id_in_operator,
    greatest_qty_unit_price_share_of_warehouse_id_sk,
    warehouse_capacity_percent_rank,
    second_moved_at,
    minutes_since_prev_moved_at
FROM first_rows
ORDER BY operator_sum_greatest_qty_unit_price, warehouse_capacity_percent_rank DESC
