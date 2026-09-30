SELECT id, revenue, store_id FROM store_sales WHERE revenue <= (SELECT MIN(revenue) FROM store_sales WHERE product_id IN (6, 7, 8, 9));
