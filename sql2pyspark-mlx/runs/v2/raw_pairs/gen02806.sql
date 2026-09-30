SELECT
    store_id,
    QUARTER(sale_date) AS sale_quarter,
    product_id,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    MAX(units) AS max_units,
    SUM(revenue) AS total_revenue
FROM store_sales
WHERE product_id IN (3, 8, 9)
GROUP BY store_id, QUARTER(sale_date), product_id
HAVING ROUND(AVG(revenue), 2) >= 79
ORDER BY sale_quarter, store_id, product_id
LIMIT 5;
