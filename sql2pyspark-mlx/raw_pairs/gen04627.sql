WITH scored AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        AVG(revenue) OVER w_recent AS revenue_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY revenue ASC NULLS LAST) AS revenue_dense_rank,
        CUME_DIST() OVER (ORDER BY units DESC NULLS FIRST) AS units_cume_dist,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('week', sale_date), store_id ORDER BY units DESC, id ASC) AS units_bucket,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS units_avg_6_prior
    FROM store_sales
    WINDOW w_recent AS (ORDER BY sale_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    CASE WHEN units_cume_dist >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS units_cume_dist_tier,
    ROUND(AVG(revenue_avg_6_prior), 2) AS avg_revenue_avg_6_prior,
    APPROX_PERCENTILE(units_cume_dist, 0.95) AS units_cume_dist_p95,
    PERCENTILE_APPROX(units_cume_dist, 0.95) AS units_cume_dist_p95_2,
    SUM(units_cume_dist) AS total_units_cume_dist,
    MAX(units_cume_dist) AS max_units_cume_dist,
    ROUND(AVG(revenue), 2) AS avg_revenue
FROM scored
GROUP BY 1
HAVING max_units_cume_dist > 0
ORDER BY max_units_cume_dist ASC;
