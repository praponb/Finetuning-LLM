SELECT
    region,
    SUM(sqft) total_sqft,
    COUNT(*) cnt,
    SUM(SUM(sqft)) OVER (ORDER BY region) AS cumulative_sqft
FROM stores
WHERE region IN ('North', 'Central', 'East')
GROUP BY region;
