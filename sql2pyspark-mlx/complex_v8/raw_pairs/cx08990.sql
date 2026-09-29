WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_ann', 'op_bee') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group,
        unit_price + qty AS unit_price_plus_qty
    FROM stock_moves
    WHERE qty >= 189
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        operator_group,
        unit_price_plus_qty,
        ROW_NUMBER() OVER w_operator_group_move_type_by AS unit_price_plus_qty_seq,
        ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY moved_at NULLS LAST, id) AS seq_no,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator_group)) AS distinct_sku_id_in_operator_group,
        ROUND(CUME_DIST() OVER w_operator_group_move_type_by, 3) AS unit_price_plus_qty_cume_dist,
        RANK() OVER (PARTITION BY operator_group, move_type ORDER BY unit_price_plus_qty NULLS LAST) AS unit_price_plus_qty_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator_group), 0), 4) AS unit_price_share_of_operator_group,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator_group), 0), 4) AS qty_share_of_operator_group,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator_group, move_type ORDER BY unit_price_plus_qty NULLS LAST), 3) AS unit_price_plus_qty_percent_rank,
        RANK() OVER w_sku_id_operator_by_moved_at AS moved_at_rank,
        LAG(moved_at) OVER (PARTITION BY sku_id, operator ORDER BY moved_at NULLS LAST, id) AS prev_moved_at,
        COUNT(*) OVER (PARTITION BY operator_group) AS rows_in_operator_group,
        ROUND(AVG(unit_price_plus_qty) OVER (PARTITION BY operator_group, move_type ORDER BY unit_price_plus_qty NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_plus_qty_avg_2,
        LAG(unit_price) OVER w_operator_group_warehouse_id AS prev_unit_price,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY operator_group, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        LAST_VALUE(unit_price_plus_qty) IGNORE NULLS OVER (PARTITION BY operator_group, move_type ORDER BY unit_price_plus_qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_plus_qty
    FROM filtered_moves
    WINDOW w_operator_group_move_type_by AS (PARTITION BY operator_group, move_type ORDER BY unit_price_plus_qty NULLS LAST, id), w_sku_id_operator_by_moved_at AS (PARTITION BY sku_id, operator ORDER BY moved_at NULLS LAST, id), w_operator_group_warehouse_id AS (PARTITION BY operator_group, warehouse_id ORDER BY unit_price DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        distinct_sku_id_in_operator_group,
        ROUND(STDDEV(unit_price_plus_qty_seq), 2) AS stddev_unit_price_plus_qty_seq,
        MIN(moved_at) AS first_moved_at,
        MAX(moved_at) AS last_moved_at,
        SUM(unit_price_plus_qty_seq) AS total_unit_price_plus_qty_seq,
        COUNT(*) AS row_count,
        SUM(CASE WHEN operator = 'op_chan' THEN last_known_unit_price_plus_qty ELSE 0 END) AS last_known_unit_price_plus_qty_operator_op_chan
    FROM metrics
    WHERE rows_in_operator_group <= 17
    GROUP BY 1
)
-- Final projection
SELECT
    distinct_sku_id_in_operator_group,
    stddev_unit_price_plus_qty_seq,
    last_moved_at,
    total_unit_price_plus_qty_seq,
    row_count,
    TO_DATE(last_moved_at) AS last_moved_at_date
FROM grouped
WHERE HOUR(last_moved_at) BETWEEN 7 AND 13
