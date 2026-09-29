SELECT
    store_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(revenue) DESC) AS rnk
FROM store_sales
WHERE revenue BETWEEN 1600 AND 3500
GROUP BY store_id
ORDER BY rnk DESC
