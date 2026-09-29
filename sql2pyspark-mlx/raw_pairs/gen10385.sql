WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-10-05'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), store_id), 4) AS revenue_share,
        NTILE(4) OVER (ORDER BY units DESC, id) AS units_bucket
    FROM filtered
    WHERE store_id IN (1, 2, 5)
),
enriched AS (
    -- Attach stores attributes
    SELECT e.*, d.opened_date
    FROM with_windows e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_per_unit,
        units_bucket,
        opened_date,
        MAX(revenue) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue,
        MIN(revenue) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue
    FROM enriched
)
-- Grouped metrics
SELECT
    product_id,
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    CASE WHEN running_sum_revenue >= 20900 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_revenue_tier,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(units, 0.95) AS units_p95,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY COUNT(*) DESC) AS row_count_dense_rank
FROM metrics
GROUP BY product_id, DATE_TRUNC('YEAR', sale_date), CASE WHEN running_sum_revenue >= 20900 THEN 'LARGE' ELSE 'SMALL' END;
