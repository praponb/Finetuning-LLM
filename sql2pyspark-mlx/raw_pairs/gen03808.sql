WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        FLOOR(revenue) AS revenue_whole
    FROM store_sales
    WHERE product_id = 1
),
with_dims AS (
    SELECT
        s.id,
        s.store_id,
        s.sale_date,
        s.revenue,
        p.store_name,
        p.sqft
    FROM valid s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
metrics AS (
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        AVG(revenue) OVER w_ordered AS revenue_avg_3_prior
    FROM with_dims
    WINDOW w_ordered AS (ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
with_dims_store_sales AS (
    SELECT e.*, p.sqft AS store_sqft
    FROM metrics e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
matched_keys AS (
    SELECT id, store_id
    FROM with_dims_store_sales
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM store_sales
    WHERE product_id = 10
),
verified AS (
    SELECT s.*
    FROM with_dims_store_sales s
    JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
with_windows AS (
    SELECT
        id,
        sale_date,
        store_name,
        sqft,
        store_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) AS sqft_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY YEAR(sale_date), store_id), 4) AS revenue_share_2
    FROM verified
)
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN sqft >= 13997 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    PERCENTILE_APPROX(store_sqft, ARRAY(0.25, 0.5, 0.75)) AS store_sqft_quartiles,
    APPROX_PERCENTILE(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY DATE_TRUNC('QUARTER', sale_date), CASE WHEN sqft >= 13997 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', sale_date)
ORDER BY distinct_ids NULLS FIRST
