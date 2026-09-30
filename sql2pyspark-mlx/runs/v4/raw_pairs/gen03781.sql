WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS LAST, id) = 1
)
SELECT
    CASE WHEN sqft >= 16503 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    AVG(sqft) AS avg_sqft,
    MIN(sqft) AS min_sqft,
    BIT_OR(sqft) AS sqft_bit_or,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50,
    ROUND(AVG(sqft), 2) AS avg_sqft_2,
    ROUND(AVG(sqft), 2) AS avg_sqft_3
FROM ranked
GROUP BY CASE WHEN sqft >= 16503 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_sqft_3 NULLS LAST;
