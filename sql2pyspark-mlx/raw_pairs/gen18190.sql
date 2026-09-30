SELECT amount AS value, 'payment' AS record_type FROM payments UNION ALL SELECT price AS value, 'product' AS record_type FROM products WHERE stock_qty < 262
