WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'EXPIRED')
      AND premium < 3817.5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER w_end_date_month_product) AS distinct_holder_id_in_end_date_month_product,
        premium - LAG(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium NULLS LAST, id) AS premium_change,
        ROUND(premium - AVG(premium) OVER w_end_date_month_product, 2) AS premium_vs_end_date_month_product_avg,
        SUM(days_since_end_date) OVER (PARTITION BY YEAR(end_date), product ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_end_date
    FROM prepared
    WINDOW w_end_date_month_product AS (PARTITION BY MONTH(end_date), product)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_holder_id_in_end_date_month_product,
    COALESCE(product, 'ALL') AS product,
    holder_id,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(running_days_since_end_date), 2) AS stddev_running_days_since_end_date,
    COUNT(*) AS row_count
FROM metrics
GROUP BY GROUPING SETS ((distinct_holder_id_in_end_date_month_product, product, holder_id), (distinct_holder_id_in_end_date_month_product), (product))
ORDER BY distinct_holder_id_in_end_date_month_product NULLS LAST, product NULLS LAST, holder_id NULLS LAST
