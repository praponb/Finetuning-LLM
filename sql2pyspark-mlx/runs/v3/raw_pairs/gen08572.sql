SELECT id, units, store_id, WEEKOFYEAR(sale_date) AS sale_date_weekofyear FROM store_sales ORDER BY 2 DESC;
