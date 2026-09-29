WITH src AS (
    SELECT *, CASE WHEN sqft >= 16298 THEN 'High' WHEN sqft >= 5993 THEN 'Medium' ELSE 'Low' END AS sqft_band
    FROM stores
    WHERE sqft < 11853 AND (UPPER(store_name) LIKE '%LD %' OR store_name LIKE 'Ri%')
),
region_summary AS (
    SELECT region, SUM(sqft) AS total_sqft
    FROM src
    GROUP BY region
)
SELECT *
FROM region_summary
WHERE total_sqft < 74000;
