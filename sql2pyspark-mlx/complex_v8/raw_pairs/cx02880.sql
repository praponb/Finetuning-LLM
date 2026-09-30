WITH scoped AS (
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
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 13
      AND warehouse_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_moved_at_year_operator AS rows_in_moved_at_year_operator,
        LEAD(moved_at) OVER (ORDER BY unit_price DESC, id) AS next_moved_at,
        NTILE(3) OVER (ORDER BY qty, id) AS qty_tercile,
        DENSE_RANK() OVER (ORDER BY unit_price_filled ASC) AS unit_price_filled_dense_rank,
        LAST_VALUE(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(AVG(qty) OVER (ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_6_prior,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_operator, 0), 4) AS unit_price_share_of_operator,
        LAG(unit_price_filled) OVER (ORDER BY unit_price_filled ASC, id) AS prev_unit_price_filled,
        MAX(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_max_2_prior,
        FIRST_VALUE(qty) OVER (PARTITION BY operator ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        LEAD(unit_price_filled, 3, 0) OVER w_all_by_unit_price AS next3_unit_price_filled,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        ROW_NUMBER() OVER w_operator_by_unit_price AS seq_no,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM scoped
    WINDOW w_moved_at_year_operator AS (PARTITION BY YEAR(moved_at), operator), w_operator AS (PARTITION BY operator), w_all_by_unit_price AS (ORDER BY unit_price DESC, id), w_operator_by_unit_price AS (PARTITION BY operator ORDER BY unit_price DESC, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(qty_avg_6_prior) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_min_qty_avg_6_prior,
        ROUND(AVG(unit_price_share_of_operator) OVER (PARTITION BY TO_DATE(moved_at)), 2) AS moved_at_to_date_avg_unit_price_share_of_operato,
        ROUND(qty_tercile / NULLIF(SUM(qty_tercile) OVER (PARTITION BY TO_DATE(moved_at)), 0), 4) AS qty_tercile_share_of_moved_at_to_date,
        LEAD(qty) OVER w_warehouse_id_by_unit_price_s AS next_qty
    FROM windowed
    WINDOW w_warehouse_id_by_unit_price_s AS (PARTITION BY warehouse_id ORDER BY unit_price_share_of_operator, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_filled,
    next_moved_at,
    qty_tercile,
    unit_price_filled_dense_rank,
    last_qty,
    qty_avg_6_prior,
    prev_unit_price_filled,
    next3_unit_price_filled,
    qty_vs_operator_avg,
    seq_no,
    second_qty,
    moved_at_to_date_avg_unit_price_share_of_operato,
    qty_tercile_share_of_moved_at_to_date,
    next_qty,
    ABS(seq_no - 5) AS seq_no_distance
FROM with_windows
ORDER BY qty_tercile, moved_at_to_date_avg_unit_price_share_of_operato DESC
