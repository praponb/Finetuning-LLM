WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty > 93
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER w_moved_at_year, 2) AS unit_price_vs_moved_at_year_avg,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC NULLS FIRST, id) AS unit_price_dense_rank,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_moved_at,
        MAX(unit_price) OVER w_moved_at_year AS moved_at_year_max_unit_price,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY YEAR(moved_at))) AS distinct_operator_in_moved_at_year,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        RANK() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY moved_at DESC) AS moved_at_rank,
        LAG(moved_at) OVER (PARTITION BY warehouse_id, MONTH(moved_at) ORDER BY unit_price DESC NULLS FIRST, id) AS prev_moved_at,
        MIN(unit_price) OVER (PARTITION BY YEAR(moved_at)) AS moved_at_year_min_unit_price,
        COUNT(*) OVER (PARTITION BY YEAR(moved_at)) AS rows_in_moved_at_year
    FROM cleaned_moves
    WINDOW w_moved_at_year AS (PARTITION BY YEAR(moved_at))
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        unit_price_vs_moved_at_year_avg,
        minutes_since_prev_moved_at,
        moved_at_year_max_unit_price,
        distinct_operator_in_moved_at_year,
        first_known_unit_price,
        moved_at_rank,
        prev_moved_at,
        moved_at_year_min_unit_price,
        rows_in_moved_at_year
    FROM ranked
    WHERE unit_price_dense_rank <= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price_vs_moved_at_year_avg,
        minutes_since_prev_moved_at,
        distinct_operator_in_moved_at_year,
        first_known_unit_price,
        moved_at_rank,
        rows_in_moved_at_year,
        MAX(minutes_since_prev_moved_at) OVER (PARTITION BY warehouse_id ORDER BY rows_in_moved_at_year ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS minutes_since_prev_moved_at_max_1_prior,
        LAG(prev_moved_at) OVER (ORDER BY minutes_since_prev_moved_at DESC, id) AS prev_prev_moved_at,
        RANK() OVER (PARTITION BY distinct_operator_in_moved_at_year ORDER BY moved_at_year_min_unit_price) AS moved_at_year_min_unit_price_rank,
        ROUND(minutes_since_prev_moved_at - AVG(minutes_since_prev_moved_at) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)), 2) AS minutes_since_prev_moved_at_vs_moved_at_month_av,
        ROUND((UNIX_TIMESTAMP(prev_moved_at) - UNIX_TIMESTAMP(LAG(prev_moved_at) OVER (PARTITION BY distinct_operator_in_moved_at_year ORDER BY moved_at_year_min_unit_price, id))) / 60, 1) AS minutes_since_prev_prev_moved_at,
        LEAD(moved_at_year_max_unit_price) OVER (PARTITION BY distinct_operator_in_moved_at_year ORDER BY moved_at_year_min_unit_price, id) AS next_moved_at_year_max_unit_price,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY rows_in_moved_at_year ASC, id) AS rows_in_moved_at_year_quintile
    FROM first_rows
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    operator,
    unit_price_vs_moved_at_year_avg,
    minutes_since_prev_moved_at,
    distinct_operator_in_moved_at_year,
    moved_at_rank,
    minutes_since_prev_moved_at_max_1_prior,
    moved_at_year_min_unit_price_rank,
    minutes_since_prev_moved_at_vs_moved_at_month_av,
    minutes_since_prev_prev_moved_at,
    next_moved_at_year_max_unit_price,
    rows_in_moved_at_year_quintile,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
    DAYOFWEEK(prev_prev_moved_at) AS prev_prev_moved_at_dayofweek
FROM with_trends
ORDER BY distinct_operator_in_moved_at_year DESC NULLS FIRST, rows_in_moved_at_year_quintile DESC
