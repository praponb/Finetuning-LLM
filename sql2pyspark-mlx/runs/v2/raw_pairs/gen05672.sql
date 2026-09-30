SELECT store_id, product_id, ROUND(AVG(units), 2) AS avg_units FROM store_sales WHERE sale_date BETWEEN '2023-12-08' AND '2025-04-01' GROUP BY store_id, product_id;
