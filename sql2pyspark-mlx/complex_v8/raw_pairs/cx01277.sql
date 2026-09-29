WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        operator,
        LEAD(moved_at_date) OVER (ORDER BY unit_price, id) AS next_moved_at_date,
        SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_sum_7,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price), 3) AS unit_price_percent_rank,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_unit_price,
        RANK() OVER (ORDER BY unit_price) AS unit_price_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_operator, 0), 4) AS unit_price_share_of_sku_id_operator,
        DENSE_RANK() OVER (ORDER BY unit_price, id) AS unit_price_dense_rank,
        NTH_VALUE(moved_at_date, 2) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_date,
        SIZE(COLLECT_SET(sku_id) OVER w_warehouse_id) AS distinct_sku_id_in_warehouse_id
    FROM scoped
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator), w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        operator,
        next_moved_at_date,
        unit_price_sum_7,
        unit_price_percent_rank,
        warehouse_id_avg_unit_price,
        unit_price_share_of_sku_id_operator,
        unit_price_dense_rank,
        second_moved_at_date,
        distinct_sku_id_in_warehouse_id
    FROM with_trends
    WHERE unit_price_rank = 1
)
SELECT
    id,
    operator,
    next_moved_at_date,
    unit_price_percent_rank,
    warehouse_id_avg_unit_price,
    unit_price_share_of_sku_id_operator,
    unit_price_dense_rank,
    distinct_sku_id_in_warehouse_id
FROM top_per_group
WHERE distinct_sku_id_in_warehouse_id <= 6
ORDER BY unit_price_dense_rank, id
LIMIT 10
