WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) = 1
)
SELECT
    CASE WHEN sqft >= 16618 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    COUNT(*) AS row_count,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    COUNT(DISTINCT city) AS distinct_cities,
    SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft
FROM ranked
GROUP BY CASE WHEN sqft >= 16618 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY sqft_tier DESC NULLS LAST;
