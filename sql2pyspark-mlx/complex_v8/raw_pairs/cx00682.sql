WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 09:00:00'
      AND sku_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_max_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id ORDER BY days_since_moved_at), 3) AS days_since_moved_at_percent_rank,
        LAG(moved_at, 3) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id) AS prev3_moved_at,
        RANK() OVER w_warehouse_id_sku_id_by_days AS days_since_moved_at_rank,
        COUNT(*) OVER (PARTITION BY operator) AS rows_in_operator,
        MAX(unit_price) OVER (PARTITION BY operator, MONTH(moved_at)) AS operator_moved_at_month_max_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, MONTH(moved_at))) AS distinct_warehouse_id_in_operator_moved_at_month,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator), 0), 4) AS unit_price_share_of_operator
    FROM prepared_moves
    WINDOW w_warehouse_id_sku_id_by_days AS (PARTITION BY warehouse_id, sku_id ORDER BY days_since_moved_at)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        moved_at,
        unit_price,
        days_since_moved_at,
        days_since_moved_at_percent_rank,
        days_since_moved_at_rank,
        operator_moved_at_month_max_unit_price,
        first_known_unit_price,
        unit_price_share_of_operator,
        ROUND(days_since_moved_at_rank - AVG(days_since_moved_at_rank) OVER w_moved_at_to_date, 2) AS days_since_moved_at_rank_vs_moved_at_to_date_avg,
        SUM(first_known_unit_price) OVER w_moved_at_to_date AS moved_at_to_date_sum_first_known_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_moved_at_to_date, 0), 4) AS unit_price_share_of_moved_at_to_date
    FROM metrics
    WINDOW w_moved_at_to_date AS (PARTITION BY TO_DATE(moved_at))
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(moved_at) AS moved_at_year,
        days_since_moved_at_rank,
        ROUND(SUM(days_since_moved_at) / NULLIF(SUM(days_since_moved_at_percent_rank), 0), 3) AS days_since_moved_at_to_days_since_moved_at_perce,
        ROUND(SUM(moved_at_to_date_sum_first_known_unit_price), 2) AS total_moved_at_to_date_sum_first_known_unit_pric,
        MIN(moved_at) AS first_moved_at,
        ROUND(AVG(days_since_moved_at_rank_vs_moved_at_to_date_avg), 2) AS avg_days_since_moved_at_rank_vs_moved_at_to_date
    FROM with_trends
    WHERE days_since_moved_at_rank < 6
    GROUP BY YEAR(moved_at), days_since_moved_at_rank
)
SELECT
    moved_at_year,
    first_moved_at,
    COALESCE(days_since_moved_at_rank, 0) AS days_since_moved_at_rank_filled
FROM rollup_base
ORDER BY days_since_moved_at_rank_filled NULLS LAST, moved_at_year NULLS LAST
