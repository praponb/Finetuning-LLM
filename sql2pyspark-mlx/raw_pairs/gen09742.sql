SELECT cost FROM products UNION SELECT revenue FROM store_sales WHERE product_id IN (2, 4, 6, 9);
