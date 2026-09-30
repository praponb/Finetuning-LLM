SELECT shipping_fee FROM orders WHERE channel NOT IN ('mobile', 'store') UNION ALL SELECT price FROM products;
