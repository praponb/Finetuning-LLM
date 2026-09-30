SELECT store_id, product_id, MIN(sale_date) AS first_sale_date, MAX(units) AS max_units FROM store_sales WHERE product_id = 6 GROUP BY store_id, product_id ORDER BY first_sale_date DESC;
