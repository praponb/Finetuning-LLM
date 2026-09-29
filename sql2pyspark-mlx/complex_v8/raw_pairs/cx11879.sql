WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
      AND unit_price > 215.4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id) AS unit_price_quintile,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY warehouse_id), 2) AS days_since_moved_at_vs_warehouse_id_avg,
        NTH_VALUE(days_since_moved_at, 2) OVER (PARTITION BY operator ORDER BY qty_minus_unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_moved_at,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY operator ORDER BY qty_minus_unit_price ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_moved_at_avg_1_prior,
        NTILE(4) OVER (PARTITION BY move_type ORDER BY qty_minus_unit_price DESC, id) AS qty_minus_unit_price_quartile,
        SIZE(COLLECT_SET(sku_id) OVER w_move_type_warehouse_id) AS distinct_sku_id_in_move_type_warehouse_id,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        LAG(qty_minus_unit_price) OVER w_sku_id_by_unit_price AS prev_qty_minus_unit_price,
        NTILE(4) OVER w_move_type_sku_id_by_unit_pri AS unit_price_quartile,
        ROUND(qty_minus_unit_price - AVG(qty_minus_unit_price) OVER w_warehouse_id, 2) AS qty_minus_unit_price_vs_warehouse_id_avg,
        LAST_VALUE(unit_price) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price, id) AS rn,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY qty_minus_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        MAX(days_since_moved_at) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_moved_at_max_2_prior,
        LAST_VALUE(days_since_moved_at) OVER (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_moved_at
    FROM scoped
    WINDOW w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id), w_sku_id_by_unit_price AS (PARTITION BY sku_id ORDER BY unit_price, id), w_move_type_sku_id_by_unit_pri AS (PARTITION BY move_type, sku_id ORDER BY unit_price DESC, id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        days_since_moved_at,
        qty_minus_unit_price,
        unit_price_quintile,
        days_since_moved_at_vs_warehouse_id_avg,
        second_days_since_moved_at,
        days_since_moved_at_avg_1_prior,
        qty_minus_unit_price_quartile,
        distinct_sku_id_in_move_type_warehouse_id,
        rows_in_warehouse_id,
        prev_qty_minus_unit_price,
        unit_price_quartile,
        qty_minus_unit_price_vs_warehouse_id_avg,
        last_unit_price,
        last_known_unit_price,
        days_since_moved_at_max_2_prior,
        last_days_since_moved_at
    FROM with_windows
    WHERE rn = 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        unit_price_quintile,
        operator,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(last_unit_price) AS max_last_unit_price
    FROM top_per_group
    WHERE days_since_moved_at_vs_warehouse_id_avg < 490.0
    GROUP BY unit_price_quintile, operator
    HAVING MAX(days_since_moved_at_max_2_prior) > 210
)
SELECT unit_price_quintile, max_last_unit_price
FROM rollup_base
ORDER BY max_last_unit_price, unit_price_quintile DESC
