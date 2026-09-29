SELECT
    MONTH(opened_date) opened_month,
    SUM(sqft) total_sqft,
    COUNT(*) cnt,
    RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk
FROM stores
GROUP BY MONTH(opened_date)
ORDER BY rnk ASC
