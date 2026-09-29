WITH valid AS (
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
    FROM stores
    WHERE opened_date >= DATE '2023-09-13'
      AND region IS NOT DISTINCT FROM 'South'
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS sqft_bucket,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share,
        NTILE(10) OVER (PARTITION BY region, city ORDER BY sqft, id) AS sqft_bucket_2,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_cume_dist
    FROM valid
),
deduped AS (
    SELECT
        id,
        opened_date,
        sqft_cume_dist
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY city, region ORDER BY sqft_cume_dist DESC) = 1
),
enriched AS (
    SELECT
        *,
        LAG(sqft_cume_dist, 1) OVER (ORDER BY sqft_cume_dist, id) AS prev_sqft_cume_dist,
        SUM(sqft_cume_dist) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date) ORDER BY sqft_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_cume_dist,
        FIRST_VALUE(sqft_cume_dist) IGNORE NULLS OVER (PARTITION BY MONTH(opened_date) ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_cume_dist,
        PERCENT_RANK() OVER (ORDER BY sqft_cume_dist DESC NULLS LAST) AS sqft_cume_dist_percent_rank
    FROM deduped
    WHERE sqft_cume_dist BETWEEN 0.45 AND 0.75
),
stats AS (
    SELECT
        CASE WHEN prev_sqft_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END AS prev_sqft_cume_dist_tier,
        PERCENTILE_APPROX(prev_sqft_cume_dist, 0.5) AS prev_sqft_cume_dist_p50,
        ROUND(AVG(prev_sqft_cume_dist), 2) AS avg_prev_sqft_cume_dist
    FROM enriched
    GROUP BY CUBE (CASE WHEN prev_sqft_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END)
    HAVING prev_sqft_cume_dist_p50 > 0
)
SELECT prev_sqft_cume_dist_p50
FROM stats
ORDER BY prev_sqft_cume_dist_p50 DESC
