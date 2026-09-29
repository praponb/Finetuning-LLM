SELECT cost AS value, 'product' AS source FROM products UNION ALL SELECT shipping_fee AS value, 'order' AS source FROM orders
