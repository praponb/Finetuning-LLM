SELECT sale_date, units FROM store_sales WHERE product_id = 9 AND (store_id IN (3, 4, 6) OR product_id IN (1, 4, 6, 7)) ORDER BY sale_date;
