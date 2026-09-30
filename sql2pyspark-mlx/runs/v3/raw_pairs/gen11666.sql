WITH Base AS (
    SELECT *, TRIM(region) AS clean_region
    FROM stores
    WHERE sqft <= 7354 AND sqft IS NOT NULL
),
summary AS (
    SELECT region, SUM(sqft) AS total_sqft
    FROM Base
    GROUP BY region
)
SELECT region, total_sqft
FROM summary
WHERE total_sqft < 80000
ORDER BY total_sqft DESC, region
LIMIT 3;
