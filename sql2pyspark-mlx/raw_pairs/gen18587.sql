SELECT id, units, sale_date, product_id FROM store_sales WHERE units >= (SELECT AVG(units) FROM store_sales WHERE store_id IN (1, 3, 4, 6))
