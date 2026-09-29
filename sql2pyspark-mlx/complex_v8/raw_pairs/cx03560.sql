WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        CASE WHEN qty >= 300 THEN 'HIGH' WHEN qty >= 139 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier
    FROM stock_moves
    WHERE qty <= 372
      AND warehouse_id >= 2
),
with_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.days_since_moved_at,
        s.qty_tier,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg,
        w.opened_on AS warehouse_opened_on
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        days_since_moved_at,
        qty_tier,
        sku_labels,
        warehouse_opened_on,
        FIRST_VALUE(sku_weight_kg) IGNORE NULLS OVER (ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sku_weight_kg,
        COUNT(*) OVER (PARTITION BY YEAR(warehouse_opened_on)) AS rows_in_warehouse_opened_on_year,
        SUM(sku_weight_kg) OVER (PARTITION BY YEAR(warehouse_opened_on)) AS warehouse_opened_on_year_sum_sku_weight_kg,
        FIRST_VALUE(moved_at) OVER (ORDER BY days_since_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        LAG(warehouse_opened_on, 2) OVER w_all_by_warehouse_opened_on AS prev2_warehouse_opened_on,
        RANK() OVER (ORDER BY days_since_moved_at ASC) AS days_since_moved_at_rank,
        MAX(days_since_moved_at) OVER (ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_moved_at_so_far,
        LEAD(warehouse_opened_on) OVER (ORDER BY days_since_moved_at ASC, id) AS next_warehouse_opened_on,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY YEAR(warehouse_opened_on)), 2) AS warehouse_opened_on_year_avg_sku_weight_kg,
        MAX(sku_weight_kg) OVER (PARTITION BY YEAR(warehouse_opened_on)) AS warehouse_opened_on_year_max_sku_weight_kg,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER w_warehouse_opened_on_year, 0), 4) AS days_since_moved_at_share_of_warehouse_opened_on
    FROM with_details
    WINDOW w_all_by_warehouse_opened_on AS (ORDER BY warehouse_opened_on, id), w_warehouse_opened_on_year AS (PARTITION BY YEAR(warehouse_opened_on))
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE days_since_moved_at_rank <= 3
      AND MONTH(first_moved_at) IN (2, 3)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        warehouse_opened_on,
        warehouse_opened_on_year_sum_sku_weight_kg,
        first_moved_at,
        prev2_warehouse_opened_on,
        days_since_moved_at_rank,
        next_warehouse_opened_on,
        warehouse_opened_on_year_avg_sku_weight_kg,
        warehouse_opened_on_year_max_sku_weight_kg,
        ROUND(PERCENT_RANK() OVER (ORDER BY warehouse_opened_on_year_sum_sku_weight_kg DESC), 3) AS warehouse_opened_on_year_sum_sku_weight_kg_perce,
        SUM(first_known_sku_weight_kg) OVER (PARTITION BY DATE_TRUNC('MONTH', prev2_warehouse_opened_on), DATE_TRUNC('MONTH', next_warehouse_opened_on)) AS prev2_warehouse_opened_on_month_next_warehouse_o,
        LAST_VALUE(days_since_moved_at_rank) IGNORE NULLS OVER (PARTITION BY YEAR(moved_at), MONTH(first_moved_at) ORDER BY first_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_moved_at_rank,
        FIRST_VALUE(days_since_moved_at) OVER (ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_moved_at,
        LAST_VALUE(moved_at) OVER (PARTITION BY YEAR(moved_at), MONTH(first_moved_at) ORDER BY first_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at
    FROM leaders
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price,
    warehouse_opened_on,
    warehouse_opened_on_year_sum_sku_weight_kg,
    first_moved_at,
    days_since_moved_at_rank,
    warehouse_opened_on_year_max_sku_weight_kg,
    prev2_warehouse_opened_on_month_next_warehouse_o,
    last_known_days_since_moved_at_rank,
    first_days_since_moved_at,
    ABS(days_since_moved_at_rank - 6) AS days_since_moved_at_rank_distance,
    days_since_moved_at_rank + unit_price AS days_since_moved_at_rank_plus_unit_price
FROM windowed
ORDER BY days_since_moved_at_rank_distance, first_moved_at
