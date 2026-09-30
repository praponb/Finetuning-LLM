SELECT MONTH(sale_date) AS sale_month, store_id, AVG(units) AS avg_units FROM store_sales GROUP BY MONTH(sale_date), store_id
