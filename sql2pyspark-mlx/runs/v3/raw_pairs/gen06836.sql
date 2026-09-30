SELECT store_id, MAX(sale_date) AS latest_sale_date, AVG(revenue) AS avg_revenue
FROM store_sales
WHERE revenue >= 3200
GROUP BY store_id
HAVING avg_revenue >= 3200
