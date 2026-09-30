WITH cleaned AS (
    -- Filter stock_moves
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
    WHERE qty <= 445
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        qty - LAG(qty) OVER (PARTITION BY operator, move_type ORDER BY qty ASC NULLS LAST, id) AS qty_change,
        SIZE(COLLECT_SET(move_type) OVER w_operator_warehouse_id) AS distinct_move_type_in_operator_warehouse_id,
        NTILE(4) OVER (ORDER BY unit_price, id) AS unit_price_quartile,
        unit_price - LAG(unit_price) OVER (ORDER BY qty, id) AS unit_price_change,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, move_type ORDER BY qty ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        NTILE(3) OVER w_move_type_by_qty AS qty_tercile,
        SUM(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) AS seq_no,
        NTH_VALUE(qty, 2) OVER (PARTITION BY operator, move_type ORDER BY qty ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        COUNT(*) OVER (PARTITION BY operator, warehouse_id) AS rows_in_operator_warehouse_id,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator, warehouse_id), 2) AS unit_price_vs_operator_warehouse_id_avg,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id), 0), 4) AS unit_price_share_of_warehouse_id,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, warehouse_id), 2) AS qty_vs_operator_warehouse_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY operator, move_type ORDER BY qty ASC NULLS LAST), 3) AS qty_cume_dist
    FROM cleaned
    WINDOW w_operator_warehouse_id AS (PARTITION BY operator, warehouse_id), w_move_type_by_qty AS (PARTITION BY move_type ORDER BY qty, id), w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    qty_change,
    unit_price_quartile,
    unit_price_change,
    unit_price_avg_1_prior,
    qty_tercile,
    second_qty,
    rows_in_operator_warehouse_id,
    unit_price_vs_operator_warehouse_id_avg,
    unit_price_share_of_warehouse_id,
    qty_vs_operator_warehouse_id_avg,
    qty_cume_dist
FROM sequenced
