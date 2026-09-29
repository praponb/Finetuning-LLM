WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM stock_moves
    WHERE unit_price >= 124.1
      AND sku_id >= 3
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_filled,
        sk.is_active AS sku_is_active,
        w.city AS warehouse_city
    FROM scoped_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price_filled NULLS LAST, id), 3) AS unit_price_filled_percent_rank,
        DENSE_RANK() OVER w_moved_at_year_sku_id_by_unit AS unit_price_filled_dense_rank,
        RANK() OVER w_all_by_unit_price_filled_tie AS unit_price_filled_rank,
        COUNT(*) OVER w_sku_is_active AS rows_in_sku_is_active,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_is_active, operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        NTH_VALUE(unit_price_filled, 2) OVER (PARTITION BY sku_is_active ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_filled,
        FIRST_VALUE(unit_price_filled) IGNORE NULLS OVER (PARTITION BY YEAR(moved_at), sku_id ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_filled,
        MIN(unit_price_filled) OVER (PARTITION BY sku_is_active) AS sku_is_active_min_unit_price_filled,
        LEAD(unit_price_filled) OVER (ORDER BY unit_price_filled ASC NULLS LAST, id) AS next_unit_price_filled,
        SUM(unit_price) OVER (PARTITION BY operator) AS operator_sum_unit_price,
        NTH_VALUE(unit_price_filled, 2) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_filled_2,
        SUM(unit_price) OVER (PARTITION BY sku_is_active, operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        ROUND(unit_price_filled - AVG(unit_price_filled) OVER w_sku_is_active, 2) AS unit_price_filled_vs_sku_is_active_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_price_filled ASC NULLS LAST), 3) AS unit_price_filled_percent_rank_2,
        MAX(unit_price_filled) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_filled_max_3_prior
    FROM enriched
    WINDOW w_moved_at_year_sku_id_by_unit AS (PARTITION BY YEAR(moved_at), sku_id ORDER BY unit_price_filled, id), w_all_by_unit_price_filled_tie AS (ORDER BY unit_price_filled ASC NULLS LAST), w_sku_is_active AS (PARTITION BY sku_is_active)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    operator,
    unit_price,
    unit_price_filled,
    sku_is_active,
    warehouse_city,
    unit_price_filled_rank,
    rows_in_sku_is_active,
    second_moved_at,
    second_unit_price_filled,
    first_known_unit_price_filled,
    next_unit_price_filled,
    operator_sum_unit_price,
    second_unit_price_filled_2,
    running_unit_price,
    unit_price_filled_vs_sku_is_active_avg,
    unit_price_filled_percent_rank_2,
    unit_price_filled_max_3_prior
FROM sequenced
ORDER BY unit_price_filled_max_3_prior
