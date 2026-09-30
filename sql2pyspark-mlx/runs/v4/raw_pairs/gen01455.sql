SELECT shipping_fee FROM orders WHERE channel NOT IN ('partner', 'mobile', 'web') UNION SELECT cost FROM products WHERE price < 1190
