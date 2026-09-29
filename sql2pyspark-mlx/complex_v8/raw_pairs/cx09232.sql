WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        ABS(qty - 148) AS qty_distance
    FROM stock_moves
    WHERE warehouse_id >= 2
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month_2,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        SUM(CASE WHEN move_type <> 'TRANSFER' THEN 1 ELSE 0 END) AS n_move_type_transfer,
        COUNT(*) AS row_count,
        SUM(qty_distance) AS total_qty_distance,
        COUNT(DISTINCT move_type) AS distinct_move_types
    FROM prepared_moves
    WHERE MONTH(moved_at_month) IN (1, 3)
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE distinct_move_types > (SELECT MIN(distinct_move_types) FROM summary)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE row_count > (SELECT AVG(row_count) FROM qualified)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_distinct_move_types_moved_at AS n_move_type_transfer_dense_rank,
        DENSE_RANK() OVER (PARTITION BY distinct_move_types, MONTH(moved_at_month_2) ORDER BY n_move_type_transfer, moved_at_month_2) AS n_move_type_transfer_dense_rank_3,
        ROUND(AVG(n_move_type_transfer) OVER (ORDER BY distinct_sku_ids, moved_at_month_2 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_move_type_transfer_avg_7,
        SUM(total_qty_distance) OVER (ORDER BY distinct_sku_ids, moved_at_month_2 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_qty_distance_sum_3_prior,
        MIN(total_qty_distance) OVER (PARTITION BY n_move_type_transfer) AS n_move_type_transfer_min_total_qty_distance
    FROM screened
    WINDOW w_distinct_move_types_moved_at AS (PARTITION BY distinct_move_types, MONTH(moved_at_month_2) ORDER BY n_move_type_transfer)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', moved_at_month_2) AS moved_at_month_2_month,
        DATE_TRUNC('YEAR', moved_at_month_2) AS moved_at_month_2_year
    FROM metrics
)
SELECT DISTINCT total_qty_distance_sum_3_prior
FROM enriched
ORDER BY total_qty_distance_sum_3_prior
