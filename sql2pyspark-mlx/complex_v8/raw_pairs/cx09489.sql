WITH recent_moves AS (
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
        MONTH(moved_at) AS moved_at_month
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'OUTBOUND', 'TRANSFER')
      AND warehouse_id <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(unit_price) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        LEAD(moved_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY unit_price, id) AS next3_moved_at,
        DENSE_RANK() OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price) AS unit_price_dense_rank,
        ROUND(AVG(qty) OVER w_moved_at_year, 2) AS moved_at_year_avg_qty,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY operator)) AS distinct_sku_id_in_operator,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        qty - LAG(qty) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY moved_at_month, id) AS qty_change,
        RANK() OVER w_all_by_unit_price AS unit_price_rank
    FROM recent_moves
    WINDOW w_moved_at_year AS (PARTITION BY YEAR(moved_at)), w_all_by_unit_price AS (ORDER BY unit_price DESC NULLS FIRST, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE unit_price_rank = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        moved_at,
        unit_price,
        moved_at_month,
        first_unit_price,
        next3_moved_at,
        unit_price_dense_rank,
        moved_at_year_avg_qty,
        distinct_sku_id_in_operator,
        second_unit_price,
        unit_price_rank,
        MAX(first_unit_price) OVER (ORDER BY second_unit_price NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS first_unit_price_max_3_prior,
        RANK() OVER w_unit_price_rank_distinct_sku AS first_unit_price_rank,
        SUM(unit_price_rank) OVER (PARTITION BY unit_price_dense_rank) AS unit_price_dense_rank_sum_unit_price_rank
    FROM top_rows
    WINDOW w_unit_price_rank_distinct_sku AS (PARTITION BY unit_price_rank, distinct_sku_id_in_operator ORDER BY first_unit_price DESC)
)
SELECT
    id,
    moved_at_month,
    first_unit_price,
    unit_price_dense_rank,
    moved_at_year_avg_qty,
    distinct_sku_id_in_operator,
    unit_price_rank,
    first_unit_price_max_3_prior,
    unit_price_dense_rank_sum_unit_price_rank
FROM with_trends
