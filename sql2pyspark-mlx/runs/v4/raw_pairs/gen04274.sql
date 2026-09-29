SELECT ss.id, ss.product_id, p.product_name FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE ss.revenue > 3000 AND ss.store_id IN (2, 5);
