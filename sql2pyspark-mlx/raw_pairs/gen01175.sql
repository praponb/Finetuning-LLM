SELECT s.* FROM products s WHERE EXISTS (SELECT 1 FROM store_sales c WHERE c.product_id = s.id AND c.revenue >= 3500)
