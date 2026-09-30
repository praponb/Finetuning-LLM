WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        cost,
        AVG(cost) OVER (ORDER BY stock_qty ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        CORR(price, stock_qty) OVER (PARTITION BY category, supplier_id) AS corr_price_stock_qty,
        AVG(cost) OVER (PARTITION BY brand, supplier_id ORDER BY stock_qty ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6_2
    FROM products
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, cost_avg_6_2 / NULLIF(corr_price_stock_qty, 0) AS cost_avg_6_2_per_corr_price_stock_qty
    FROM with_windows
    WHERE corr_price_stock_qty <= -0.4
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN cost >= 650 THEN 'High' WHEN cost >= 270 THEN 'Medium' ELSE 'Low' END AS cost_band
    FROM valid
    WHERE cost_avg_6_2_per_corr_price_stock_qty BETWEEN 22 AND 68
),
with_windows_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost_avg_6_2) OVER (ORDER BY cost_avg_6_2_per_corr_price_stock_qty ASC, id ASC) AS prev_cost_avg_6_2,
        LAG(cost_avg_6) OVER (ORDER BY cost_avg_6_2_per_corr_price_stock_qty ASC, id ASC) AS prev_cost_avg_6,
        LEAD(cost_avg_6) OVER (ORDER BY cost_avg_6_2_per_corr_price_stock_qty ASC, id ASC) AS next_cost_avg_6,
        ROW_NUMBER() OVER (ORDER BY cost_avg_6_2_per_corr_price_stock_qty ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY cost_avg_6_2 DESC, id ASC) AS event_seq_2
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        cost_avg_6,
        corr_price_stock_qty,
        cost_band,
        prev_cost_avg_6_2,
        prev_cost_avg_6,
        next_cost_avg_6,
        event_seq,
        event_seq_2,
        CAST(event_seq_2 AS STRING) AS event_seq_2_str,
        corr_price_stock_qty * prev_cost_avg_6_2 AS corr_price_stock_qty_times_prev_cost_avg_6_2
    FROM with_windows_products
    WHERE next_cost_avg_6 < 286
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_cost_avg_6_2 >= 581 THEN 'HIGH' ELSE 'LOW' END AS prev_cost_avg_6_2_tier,
    CASE WHEN prev_cost_avg_6 >= 711 THEN 'LARGE' ELSE 'SMALL' END AS prev_cost_avg_6_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(corr_price_stock_qty_times_prev_cost_avg_6_2), 2) AS avg_corr_price_stock_qty_times_prev_cost_avg_6_2,
    APPROX_PERCENTILE(cost_avg_6, 0.9) AS cost_avg_6_p90
FROM base
GROUP BY 1, 2
ORDER BY distinct_ids DESC NULLS LAST
