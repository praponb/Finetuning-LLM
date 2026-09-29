WITH deduped AS (
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC, id) = 1
)
SELECT
    region,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    APPROX_PERCENTILE(sqft, 0.9) AS sqft_p90,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    RANK() OVER (PARTITION BY region ORDER BY STDDEV_SAMP(sqft) DESC) AS sqft_stddev_rank
FROM deduped
GROUP BY 1, 2
HAVING COUNT(CASE WHEN city = 'Singapore' THEN 1 END) < 1000
ORDER BY sqft_p90 DESC NULLS FIRST
