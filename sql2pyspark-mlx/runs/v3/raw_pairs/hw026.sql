SELECT store_id, product_id, SUM(units) AS total_units, AVG(revenue) AS avg_revenue
FROM store_sales
GROUP BY store_id, product_id;
