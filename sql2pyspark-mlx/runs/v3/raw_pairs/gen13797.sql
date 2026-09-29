SELECT
    QUARTER(ss.sale_date) AS sale_quarter,
    YEAR(ss.sale_date) AS sale_year,
    COUNT(DISTINCT i.quantity) AS unique_stores,
    ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY QUARTER(ss.sale_date), YEAR(ss.sale_date)
ORDER BY avg_quantity;
