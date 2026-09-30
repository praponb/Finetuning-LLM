SELECT cost AS value, 'product' AS origin FROM products WHERE category = 'Electronics' UNION ALL SELECT amount AS value, 'transaction' AS origin FROM transactions;
