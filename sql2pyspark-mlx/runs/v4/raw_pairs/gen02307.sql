SELECT shipping_fee FROM orders WHERE store_id IS NULL UNION ALL SELECT cost FROM products WHERE price BETWEEN 630 AND 800;
