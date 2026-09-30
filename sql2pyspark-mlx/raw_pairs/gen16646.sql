SELECT store_id, SUM(units) AS total_units, MAX(sale_date) AS latest_sale_date FROM store_sales GROUP BY store_id
