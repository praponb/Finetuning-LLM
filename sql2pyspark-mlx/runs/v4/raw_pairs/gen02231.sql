WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        CASE WHEN sqft >= 16438 THEN 'High' WHEN sqft >= 4846 THEN 'Medium' ELSE 'Low' END AS sqft_level
    FROM stores
    WHERE region IS NOT NULL
)
SELECT
    city,
    CASE WHEN sqft >= 13947 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    SUM(sqft) AS total_sqft,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS sqft_per_tokyo,
    BIT_OR(sqft) AS sqft_bit_or,
    MAX(sqft) AS max_sqft,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM filtered
GROUP BY 1, 2
