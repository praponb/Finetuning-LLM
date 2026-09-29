WITH recent AS (
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
        LEAST(unit_price, qty) AS least_unit_price_qty,
        UPPER(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE warehouse_id >= 3
      AND sku_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        least_unit_price_qty,
        operator_clean,
        ROUND(AVG(least_unit_price_qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS least_unit_price_qty_avg_3,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type), 2) AS qty_vs_move_type_avg,
        RANK() OVER (ORDER BY qty) AS qty_rank,
        ROUND(least_unit_price_qty / NULLIF(SUM(least_unit_price_qty) OVER (PARTITION BY move_type), 0), 4) AS least_unit_price_qty_share_of_move_type,
        DENSE_RANK() OVER w_all_by_qty AS qty_dense_rank,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type) AS distinct_sku_id_in_move_type,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        RANK() OVER (ORDER BY least_unit_price_qty) AS least_unit_price_qty_rank,
        NTILE(3) OVER (ORDER BY qty, id) AS qty_tercile,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROUND(AVG(least_unit_price_qty) OVER (ORDER BY least_unit_price_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS least_unit_price_qty_avg_3_2,
        MAX(qty) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS qty_max_6_prior,
        ROUND(AVG(unit_price) OVER (ORDER BY least_unit_price_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior,
        SUM(least_unit_price_qty) OVER (ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS least_unit_price_qty_sum_3
    FROM recent
    WINDOW w_all_by_qty AS (ORDER BY qty, id), w_move_type AS (PARTITION BY move_type)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE qty_rank <= 3
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    unit_price,
    least_unit_price_qty,
    operator_clean,
    least_unit_price_qty_avg_3,
    qty_rank,
    qty_dense_rank,
    distinct_sku_id_in_move_type,
    last_known_unit_price,
    qty_tercile,
    rows_in_move_type,
    least_unit_price_qty_avg_3_2,
    qty_max_6_prior,
    unit_price_avg_3_prior
FROM leaders
ORDER BY qty_tercile
