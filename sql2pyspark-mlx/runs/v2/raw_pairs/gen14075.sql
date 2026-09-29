SELECT
    city,
    SUM(sqft) AS total_sqft,
    COUNT(*) AS cnt,
    SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft
FROM stores
GROUP BY city
ORDER BY cumulative_sqft DESC, total_sqft DESC;
