WITH base AS (
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
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE (sku_id <= 8 OR unit_price < 330.9)
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.operator_or_unknown,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        sk.is_active AS sku_is_active
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(unit_price) OVER w_sku_id_warehouse_city AS sku_id_warehouse_city_sum_unit_price,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_4,
        SUM(unit_price) OVER (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_sum_1_prior,
        NTILE(10) OVER (ORDER BY unit_price DESC NULLS FIRST, id) AS unit_price_decile,
        MIN(unit_price) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_unit_price,
        LAG(unit_price, 3) OVER (PARTITION BY warehouse_city, operator ORDER BY unit_price, id) AS prev3_unit_price,
        LEAD(unit_price, 1, 0) OVER w_operator_sku_id_by_unit_pric AS next_unit_price,
        ROUND(AVG(unit_price) OVER w_sku_id_operator, 2) AS sku_id_operator_avg_unit_price,
        ROW_NUMBER() OVER w_warehouse_city_sku_id_by_uni_3 AS rn,
        SUM(unit_price) OVER (PARTITION BY operator, operator_or_unknown ORDER BY unit_price NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC NULLS FIRST, id) AS unit_price_seq,
        DENSE_RANK() OVER (PARTITION BY operator, sku_id ORDER BY unit_price) AS unit_price_dense_rank,
        SUM(unit_price) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS unit_price_sum_1_prior_2,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_city, warehouse_id)) AS distinct_sku_id_in_warehouse_city_warehouse_id,
        LAG(unit_price) OVER (ORDER BY unit_price ASC, id) AS prev_unit_price,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior
    FROM enriched_warehouse
    WINDOW w_sku_id_warehouse_city AS (PARTITION BY sku_id, warehouse_city), w_operator_sku_id_by_unit_pric AS (PARTITION BY operator, sku_id ORDER BY unit_price, id), w_sku_id_operator AS (PARTITION BY sku_id, operator), w_warehouse_city_sku_id_by_uni_3 AS (PARTITION BY warehouse_city, sku_id ORDER BY unit_price, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        operator_or_unknown,
        warehouse_city,
        warehouse_wh_code,
        sku_is_active,
        sku_id_warehouse_city_sum_unit_price,
        unit_price_avg_4,
        unit_price_sum_1_prior,
        unit_price_decile,
        sku_id_operator_min_unit_price,
        prev3_unit_price,
        next_unit_price,
        sku_id_operator_avg_unit_price,
        running_unit_price,
        unit_price_seq,
        unit_price_dense_rank,
        unit_price_sum_1_prior_2,
        distinct_sku_id_in_warehouse_city_warehouse_id,
        prev_unit_price,
        unit_price_avg_3_prior
    FROM metrics
    WHERE rn = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type) AS distinct_warehouse_id_in_move_type,
        SUM(unit_price_sum_1_prior_2) OVER (PARTITION BY unit_price_seq ORDER BY prev_unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_sum_1_prior_2_sum_2_prior,
        ROW_NUMBER() OVER (PARTITION BY operator_or_unknown, unit_price_decile ORDER BY unit_price_decile DESC, id) AS rn,
        FIRST_VALUE(prev_unit_price) OVER (PARTITION BY unit_price_seq ORDER BY prev_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_unit_price
    FROM top_rows
    WINDOW w_move_type AS (PARTITION BY move_type)
),
grouped AS (
    -- Group-level totals
    SELECT
        unit_price_decile,
        COUNT(*) AS n_rows,
        SUM(distinct_warehouse_id_in_move_type) AS total_distinct_warehouse_id_in_move_type
    FROM with_trends
    WHERE prev_unit_price <= 396.4
    GROUP BY unit_price_decile
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        unit_price_decile,
        n_rows,
        total_distinct_warehouse_id_in_move_type,
        LAG(unit_price_decile) OVER (PARTITION BY n_rows, unit_price_decile ORDER BY n_rows, unit_price_decile) AS prev_unit_price_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_rows, unit_price_decile ORDER BY n_rows, unit_price_decile), 3) AS n_rows_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows, unit_price_decile ORDER BY n_rows), 3) AS n_rows_cume_dist
    FROM grouped
)
-- Final projection
SELECT
    unit_price_decile,
    n_rows,
    total_distinct_warehouse_id_in_move_type,
    n_rows_percent_rank,
    n_rows_cume_dist
FROM with_windows
ORDER BY n_rows_cume_dist, unit_price_decile DESC NULLS FIRST
