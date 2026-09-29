WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref,
        QUARTER(moved_at) AS moved_at_quarter,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM prepared_moves
    WHERE qty > 152
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        DATE_TRUNC('MONTH', moved_at_month) AS moved_at_month_month,
        move_type,
        SUM(CASE WHEN move_type <> 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound,
        COUNT(*) FILTER (WHERE move_type = 'TRANSFER') AS n_move_type_transfer,
        ROUND(STDDEV(moved_at_quarter), 2) AS stddev_moved_at_quarter,
        MIN(moved_at_hour) AS first_moved_at_hour
    FROM enriched
    GROUP BY TO_DATE(moved_at), DATE_TRUNC('MONTH', moved_at_month), move_type
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(stddev_moved_at_quarter) OVER (PARTITION BY n_move_type_outbound, move_type ORDER BY first_moved_at_hour DESC, moved_at_day, moved_at_month_month, move_type ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS stddev_moved_at_quarter_sum_2,
        SUM(n_move_type_transfer) OVER (PARTITION BY n_move_type_outbound, move_type ORDER BY first_moved_at_hour DESC, moved_at_day, moved_at_month_month, move_type ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS n_move_type_transfer_sum_3_prior,
        FIRST_VALUE(stddev_moved_at_quarter) OVER (PARTITION BY n_move_type_outbound, move_type ORDER BY first_moved_at_hour DESC, moved_at_day, moved_at_month_month, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_stddev_moved_at_quarter,
        MAX(n_move_type_outbound) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_day), YEAR(moved_at_month_month)) AS moved_at_day_month_moved_at_month_month_year_max
    FROM per_group
)
SELECT moved_at_day, moved_at_month_month, metric_name, amount
FROM (
    SELECT moved_at_day, moved_at_month_month, stddev_moved_at_quarter_sum_2, stddev_moved_at_quarter
    FROM sequenced
) src
UNPIVOT (
    amount FOR metric_name IN (stddev_moved_at_quarter_sum_2 AS stddev_moved_at_quarter_sum_2_v, stddev_moved_at_quarter AS stddev_moved_at_quarter_v)
) unp
