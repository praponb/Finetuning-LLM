SELECT store_id, SUM(units) AS total_units, MAX(sale_date) AS last_sale_date FROM store_sales WHERE sale_date >= '2024-07-19' GROUP BY 1 HAVING SUM(units) > 70
