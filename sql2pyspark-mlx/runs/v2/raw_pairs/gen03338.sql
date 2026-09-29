SELECT product_id, AVG(units) AS avg_units, MAX(sale_date) AS latest_sale_date FROM store_sales GROUP BY 1 ORDER BY product_id DESC LIMIT 1;
