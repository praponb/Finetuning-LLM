WITH joined AS (
    SELECT
        s.*,
        d.sqft,
        d.region,
        d.city
    FROM store_sales s
    INNER JOIN stores d
        ON s.store_id = d.id
    WHERE d.opened_date BETWEEN '2024-04-01' AND '2025-09-01'
),
enriched AS (
    SELECT
        id,
        product_id,
        units,
        sqft,
        region,
        NTILE(10) OVER (PARTITION BY city ORDER BY revenue DESC, id) AS revenue_bucket,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS revenue_bucket_2,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), city ORDER BY revenue, id) AS revenue_bucket_3
    FROM joined
),
latest AS (
    SELECT
        id,
        product_id,
        sqft,
        region,
        revenue_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, product_id ORDER BY revenue_bucket_2 DESC NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
enriched_store_sales AS (
    SELECT
        id,
        sqft,
        MIN(revenue_bucket) OVER w_recent AS running_min_revenue_bucket,
        AVG(revenue_bucket) OVER w_recent AS running_avg_revenue_bucket,
        MAX(revenue_bucket) OVER w_recent AS running_max_revenue_bucket,
        SUM(sqft) OVER w_running AS running_sqft
    FROM latest
    WINDOW w_recent AS (PARTITION BY region, product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        id,
        sqft,
        running_min_revenue_bucket,
        running_avg_revenue_bucket,
        running_max_revenue_bucket,
        running_sqft,
        NTILE(3) OVER w_recent AS running_avg_revenue_bucket_bucket,
        LEAD(running_max_revenue_bucket, 2) OVER (ORDER BY running_min_revenue_bucket, id) AS next_running_max_revenue_bucket,
        ROW_NUMBER() OVER w_hist AS row_seq,
        AVG(running_avg_revenue_bucket) OVER (ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_avg_revenue_bucket_avg_6,
        AVG(running_min_revenue_bucket) OVER (ORDER BY running_avg_revenue_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_min_revenue_bucket_avg_6_prior
    FROM enriched_store_sales
    WINDOW w_recent AS (ORDER BY running_avg_revenue_bucket, id), w_hist AS (ORDER BY sqft DESC, id)
)
SELECT
    CASE WHEN running_min_revenue_bucket >= 7.1 THEN 'LARGE' ELSE 'SMALL' END AS running_min_revenue_bucket_tier,
    CASE WHEN running_min_revenue_bucket_avg_6_prior >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_revenue_bucket_avg_6_prior_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(next_running_max_revenue_bucket), 2) AS avg_next_running_max_revenue_bucket,
    GROUPING_ID() AS grouping_id
FROM scored
GROUP BY ROLLUP (CASE WHEN running_min_revenue_bucket >= 7.1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_min_revenue_bucket_avg_6_prior >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING ROUND(AVG(next_running_max_revenue_bucket), 2) > 0
ORDER BY running_min_revenue_bucket_avg_6_prior_tier
