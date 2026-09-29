WITH enriched AS (
    SELECT
        id,
        city,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        CUME_DIST() OVER (ORDER BY sqft NULLS FIRST) AS sqft_cume_dist,
        NTILE(3) OVER (PARTITION BY city, region ORDER BY sqft, id) AS sqft_bucket,
        LAG(sqft, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_sqft
    FROM stores
),
ranked AS (
    SELECT
        id,
        sqft_avg_2,
        sqft_cume_dist,
        sqft_bucket,
        prev_sqft
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft_avg_2) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    SELECT id, COALESCE(sqft_cume_dist, 0) AS sqft_cume_dist_filled
    FROM ranked
    WHERE sqft_cume_dist > 0.72
),
enriched_stores AS (
    SELECT
        *,
        AVG(sqft_cume_dist_filled) OVER (ORDER BY sqft_cume_dist_filled, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_cume_dist_filled_avg_5,
        NTILE(4) OVER (ORDER BY sqft_cume_dist_filled, id) AS sqft_cume_dist_filled_bucket
    FROM filtered
),
stats AS (
    SELECT
        CASE WHEN sqft_cume_dist_filled_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_cume_dist_filled_bucket_tier,
        PERCENTILE_APPROX(sqft_cume_dist_filled, 0.95) AS sqft_cume_dist_filled_p95,
        MAX(sqft_cume_dist_filled) AS max_sqft_cume_dist_filled
    FROM enriched_stores
    GROUP BY CASE WHEN sqft_cume_dist_filled_bucket >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    sqft_cume_dist_filled_bucket_tier,
    sqft_cume_dist_filled_p95,
    max_sqft_cume_dist_filled
FROM stats
ORDER BY sqft_cume_dist_filled_p95 DESC;
