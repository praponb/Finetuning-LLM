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
        unit_price
    FROM stock_moves
    WHERE operator IN ('op_chan', 'op_bee')
      AND sku_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator ORDER BY qty ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_3_prior
    FROM recent_moves
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', last_moved_at) AS days_since_last_moved_at,
        CASE WHEN move_type IN ('INBOUND', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group,
        CASE WHEN qty >= 357 THEN 'large' WHEN qty >= 102 THEN 'medium' ELSE 'small' END AS qty_tier
    FROM with_windows
),
grouped AS (
    -- Group-level totals
    SELECT
        operator,
        qty_tier,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        ROUND(SUM(unit_price), 2) AS total_unit_price,
        SUM(qty) AS total_qty,
        PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price,
        ROUND(AVG(qty), 2) AS avg_qty,
        ROUND(STDDEV(unit_price_avg_3_prior), 2) AS stddev_unit_price_avg_3_prior
    FROM enriched
    GROUP BY operator, qty_tier, DATE_TRUNC('MONTH', moved_at)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE avg_qty > (SELECT AVG(avg_qty) FROM grouped)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_moved_at_month_month_qty_tie AS total_unit_price_dense_rank,
        ROUND(avg_qty / NULLIF(SUM(avg_qty) OVER (PARTITION BY operator), 0), 4) AS avg_qty_share_of_operator,
        FIRST_VALUE(total_qty) OVER (PARTITION BY MONTH(moved_at_month), qty_tier ORDER BY median_unit_price DESC, operator, qty_tier, moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_qty,
        ROUND(PERCENT_RANK() OVER (ORDER BY stddev_unit_price_avg_3_prior, operator, qty_tier, moved_at_month), 3) AS stddev_unit_price_avg_3_prior_percent_rank,
        RANK() OVER (ORDER BY stddev_unit_price_avg_3_prior, operator, qty_tier, moved_at_month) AS stddev_unit_price_avg_3_prior_rank,
        NTH_VALUE(median_unit_price, 2) OVER (ORDER BY stddev_unit_price_avg_3_prior, operator, qty_tier, moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_median_unit_price
    FROM screened
    WINDOW w_moved_at_month_month_qty_tie AS (PARTITION BY MONTH(moved_at_month), qty_tier ORDER BY total_unit_price DESC)
),
top_per_group AS (
    -- First row per group
    SELECT
        operator,
        qty_tier,
        moved_at_month,
        total_unit_price,
        total_qty,
        median_unit_price,
        avg_qty,
        stddev_unit_price_avg_3_prior,
        total_unit_price_dense_rank,
        avg_qty_share_of_operator,
        first_total_qty,
        stddev_unit_price_avg_3_prior_percent_rank,
        second_median_unit_price
    FROM with_windows_2
    WHERE stddev_unit_price_avg_3_prior_rank = 1
)
-- Subtotals with ROLLUP
SELECT
    total_unit_price_dense_rank,
    COALESCE(operator, 'ALL') AS operator,
    COALESCE(qty_tier, 'ALL') AS qty_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT operator) AS distinct_operators,
    ROUND(SUM(first_total_qty) / NULLIF(SUM(stddev_unit_price_avg_3_prior_percent_rank), 0), 3) AS first_total_qty_to_stddev_unit_price_avg_3_prior
FROM top_per_group
GROUP BY ROLLUP(total_unit_price_dense_rank, operator, qty_tier)
ORDER BY total_unit_price_dense_rank NULLS LAST, operator NULLS LAST, qty_tier NULLS LAST
