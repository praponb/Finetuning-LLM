SELECT shipping_fee, 'order' AS source FROM orders UNION ALL SELECT price, 'product' AS source FROM products WHERE stock_qty >= 267 ORDER BY source;
