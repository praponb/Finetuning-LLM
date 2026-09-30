WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
    WHERE (operator = 'op_eve' OR move_type IS NOT NULL)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, operator), 0), 4) AS unit_price_share_of_move_type_operator,
        SIZE(COLLECT_SET(operator) OVER w_move_type_moved_at_to_date) AS distinct_operator_in_move_type_moved_at_to_date,
        LEAD(unit_price, 2) OVER w_warehouse_id_moved_at_month AS next2_unit_price,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY moved_at, id) AS moved_at_seq,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_type, operator), 0), 4) AS qty_share_of_move_type_operator,
        ROUND(CUME_DIST() OVER w_warehouse_id_by_unit_price, 3) AS unit_price_cume_dist,
        SUM(qty) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        qty - LAG(qty) OVER w_move_type_by_moved_at AS qty_change,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, operator)) AS distinct_sku_id_in_move_type_operator
    FROM cleaned
    WINDOW w_move_type_moved_at_to_date AS (PARTITION BY move_type, TO_DATE(moved_at)), w_warehouse_id_moved_at_month AS (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY moved_at DESC, id), w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price DESC, id), w_move_type_by_moved_at AS (PARTITION BY move_type ORDER BY moved_at, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price_share_of_move_type_operator,
        distinct_operator_in_move_type_moved_at_to_date,
        next2_unit_price,
        moved_at_seq,
        qty_share_of_move_type_operator,
        unit_price_cume_dist,
        running_qty,
        qty_change,
        distinct_sku_id_in_move_type_operator,
        ABS(next2_unit_price - 273.5) AS next2_unit_price_distance,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM sequenced
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        qty,
        moved_at,
        unit_price_share_of_move_type_operator,
        next2_unit_price,
        moved_at_seq,
        unit_price_cume_dist,
        distinct_sku_id_in_move_type_operator,
        move_type_ref,
        days_since_moved_at,
        MIN(next2_unit_price_distance) OVER w_move_type_sku_id AS move_type_sku_id_min_next2_unit_price_distance,
        COUNT(*) OVER (PARTITION BY move_type, sku_id) AS rows_in_move_type_sku_id,
        FIRST_VALUE(days_since_moved_at) OVER (ORDER BY distinct_operator_in_move_type_moved_at_to_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type ORDER BY distinct_operator_in_move_type_moved_at_to_date DESC, id), 3) AS distinct_operator_in_move_type_moved_at_to_date_,
        ROUND(AVG(next2_unit_price_distance) OVER (ORDER BY distinct_operator_in_move_type_moved_at_to_date NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS next2_unit_price_distance_avg_3,
        MAX(unit_price_cume_dist) OVER (PARTITION BY move_type ORDER BY distinct_operator_in_move_type_moved_at_to_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_cume_dist_max_3_prior
    FROM scored
    WINDOW w_move_type_sku_id AS (PARTITION BY move_type, sku_id)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    unit_price_share_of_move_type_operator,
    next2_unit_price,
    moved_at_seq,
    distinct_sku_id_in_move_type_operator,
    days_since_moved_at,
    move_type_sku_id_min_next2_unit_price_distance,
    rows_in_move_type_sku_id,
    distinct_operator_in_move_type_moved_at_to_date_,
    unit_price_cume_dist_max_3_prior
FROM ranked
ORDER BY move_type_sku_id_min_next2_unit_price_distance DESC, unit_price_cume_dist_max_3_prior DESC
