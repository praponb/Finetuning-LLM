WITH metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        RANK() OVER (PARTITION BY MONTH(sale_date), product_id ORDER BY units NULLS FIRST) AS units_rank,
        AVG(revenue) OVER (PARTITION BY YEAR(sale_date), product_id ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        NTILE(10) OVER (PARTITION BY MONTH(sale_date), product_id ORDER BY units DESC, id) AS units_bucket
    FROM store_sales
    WHERE store_id IN (3, 4, 6)
),
cleaned AS (
    SELECT
        *,
        ROUND(units_bucket, 1) AS units_bucket_rounded,
        CAST(units_rank AS DOUBLE) AS units_rank_num
    FROM metrics
    WHERE sale_date >= DATE '2023-10-02'
),
deduped AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_bucket_rounded DESC NULLS LAST, id) = 1
),
enriched AS (
    SELECT
        id,
        store_id,
        sale_date,
        units_rank,
        units_bucket,
        units_rank_num,
        AVG(revenue_avg_3_prior) OVER (PARTITION BY store_id ORDER BY units_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_revenue_avg_3_prior,
        MAX(revenue_avg_3_prior) OVER (PARTITION BY store_id ORDER BY units_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_revenue_avg_3_prior,
        MIN(revenue_avg_3_prior) OVER (PARTITION BY store_id ORDER BY units_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_revenue_avg_3_prior
    FROM deduped
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_rank DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
)
SELECT
    CASE WHEN last7_max_revenue_avg_3_prior >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_max_revenue_avg_3_prior_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    APPROX_PERCENTILE(units_bucket, ARRAY(0.25, 0.5, 0.75)) AS units_bucket_quartiles,
    PERCENTILE_APPROX(units_rank, 0.9) AS units_rank_p90,
    COUNT(*) AS event_count
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN last7_max_revenue_avg_3_prior >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', sale_date)), (CASE WHEN last7_max_revenue_avg_3_prior >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY week_start NULLS FIRST;
