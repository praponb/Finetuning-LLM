WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_eve', 'op_chan') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 07:00:00'
      AND unit_price < 293.2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        operator_group,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_share_of_sku_id,
        COUNT(*) OVER w_warehouse_id_operator AS rows_in_warehouse_id_operator,
        MIN(qty) OVER w_sku_id AS sku_id_min_qty,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        MAX(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at), warehouse_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id, sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        ROUND(unit_price - AVG(unit_price) OVER w_warehouse_id_operator, 2) AS unit_price_vs_warehouse_id_operator_avg,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        NTILE(10) OVER (ORDER BY unit_price DESC, id) AS unit_price_decile,
        LAG(moved_at, 3) OVER w_all_by_unit_price AS prev3_moved_at
    FROM prepared
    WINDOW w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator), w_sku_id AS (PARTITION BY sku_id), w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        prev3_moved_at + INTERVAL 2 HOURS AS prev3_moved_at_plus_2h
    FROM windowed
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        operator_group,
        unit_price_share_of_sku_id,
        sku_id_min_qty,
        last_known_moved_at,
        second_moved_at,
        unit_price_vs_warehouse_id_operator_avg,
        last_known_unit_price,
        prev3_moved_at,
        move_type_or_unknown,
        ROW_NUMBER() OVER (PARTITION BY YEAR(prev3_moved_at) ORDER BY rows_in_warehouse_id_operator DESC NULLS FIRST, id) AS rn,
        FIRST_VALUE(qty_vs_sku_id_avg) OVER (PARTITION BY move_type_or_unknown ORDER BY unit_price_share_of_sku_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_vs_sku_id_avg,
        ROW_NUMBER() OVER (PARTITION BY move_type_or_unknown ORDER BY unit_price_share_of_sku_id, id) AS unit_price_share_of_sku_id_seq,
        qty_vs_sku_id_avg - LAG(qty_vs_sku_id_avg) OVER (PARTITION BY move_type_or_unknown ORDER BY unit_price_share_of_sku_id, id) AS qty_vs_sku_id_avg_change
    FROM annotated
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(second_moved_at) AS second_moved_at_day,
        DATE_TRUNC('MONTH', prev3_moved_at) AS prev3_moved_at_month,
        operator_group,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        MIN(last_known_moved_at) AS first_last_known_moved_at,
        COUNT(*) AS row_count
    FROM windowed_2
    GROUP BY 1, 2, 3
    HAVING MAX(sku_id_min_qty) > 1501
)
SELECT
    second_moved_at_day,
    avg_unit_price,
    first_last_known_moved_at,
    ABS(row_count - 5) AS row_count_distance,
    prev3_moved_at_month + INTERVAL 7 DAYS AS prev3_moved_at_month_plus_7d
FROM per_group
ORDER BY prev3_moved_at_month_plus_7d DESC, avg_unit_price DESC
