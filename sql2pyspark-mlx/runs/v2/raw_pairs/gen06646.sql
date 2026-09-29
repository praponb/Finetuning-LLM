SELECT id, store_id, sale_date, units, SUM(units) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS running_units FROM store_sales ORDER BY store_id DESC;
