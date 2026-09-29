SELECT
    QUARTER(sale_date) AS sale_quarter,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(revenue) DESC) AS rnk
FROM store_sales
WHERE store_id IN (4, 6)
GROUP BY QUARTER(sale_date)
ORDER BY rnk
