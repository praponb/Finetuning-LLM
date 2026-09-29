WITH remaining_keys AS (
    SELECT region, city
    FROM stores
    EXCEPT DISTINCT
    SELECT region, city
    FROM stores
    WHERE sqft = 12226
),
kept_rows AS (
    SELECT s.*
    FROM stores s
    JOIN remaining_keys k ON s.region = k.region AND s.city = k.city
)
SELECT
    region,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'Central' THEN 1 END), 0), 2) AS sqft_per_central,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(region) AS grouping_level
FROM kept_rows
GROUP BY CUBE (region, DATE_TRUNC('QUARTER', opened_date))
ORDER BY distinct_ids DESC NULLS LAST;
