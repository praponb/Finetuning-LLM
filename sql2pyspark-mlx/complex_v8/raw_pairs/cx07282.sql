WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month
    FROM stock_moves
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.moved_at_month,
        sk.category AS sku_category,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM base s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.category = 'Electronics'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id)) AS distinct_move_type_in_sku_id,
        LAST_VALUE(warehouse_opened_on) IGNORE NULLS OVER (PARTITION BY warehouse_city ORDER BY moved_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_warehouse_opened_on,
        SUM(unit_price) OVER (PARTITION BY moved_at_month ORDER BY unit_price ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS unit_price_sum_2,
        DENSE_RANK() OVER (PARTITION BY moved_at_month ORDER BY unit_price ASC NULLS LAST) AS unit_price_dense_rank,
        ROUND(AVG(moved_at_month) OVER (PARTITION BY sku_category, operator ORDER BY unit_price ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS moved_at_month_avg_1_prior,
        MAX(moved_at_month) OVER (PARTITION BY warehouse_city ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_moved_at_month_so_far,
        FIRST_VALUE(moved_at_month) IGNORE NULLS OVER (PARTITION BY warehouse_city ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_month,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY moved_at_month, sku_category), 2) AS unit_price_vs_moved_at_month_sku_category_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY sku_category, operator ORDER BY unit_price ASC), 3) AS unit_price_percent_rank
    FROM with_details
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', warehouse_opened_on) AS days_since_warehouse_opened_on
    FROM sequenced
)
-- Final projection
SELECT
    id,
    operator,
    unit_price,
    moved_at_month,
    sku_category,
    warehouse_city,
    distinct_move_type_in_sku_id,
    last_known_warehouse_opened_on,
    unit_price_sum_2,
    unit_price_dense_rank,
    moved_at_month_avg_1_prior,
    unit_price_vs_moved_at_month_sku_category_avg,
    unit_price_percent_rank,
    days_since_warehouse_opened_on
FROM derived
ORDER BY sku_category
