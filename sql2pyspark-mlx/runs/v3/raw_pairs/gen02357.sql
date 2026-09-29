SELECT store_id, AVG(units) avg_units FROM store_sales WHERE sale_date < '2023-08-18' GROUP BY store_id ORDER BY store_id DESC, avg_units DESC LIMIT 1;
