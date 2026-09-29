SELECT id, units, product_id, store_id FROM store_sales WHERE units > (SELECT MAX(units) FROM store_sales);
