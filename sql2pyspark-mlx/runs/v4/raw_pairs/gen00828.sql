WITH kept_keys AS (
    SELECT region, city
    FROM stores
    INTERSECT
    SELECT region, city
    FROM stores
    WHERE sqft >= 5962
),
kept_rows AS (
    SELECT s.*
    FROM stores s
    JOIN kept_keys k ON s.region = k.region AND s.city = k.city
)
SELECT
    CASE WHEN sqft >= 15579 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    city,
    COUNT(*) AS num_rows,
    MAX(sqft) AS max_sqft,
    COUNT(DISTINCT region) AS distinct_regions,
    RANK() OVER (PARTITION BY CASE WHEN sqft >= 15579 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT region) DESC) AS distinct_regions_rank
FROM kept_rows
GROUP BY 1, 2
ORDER BY num_rows DESC
