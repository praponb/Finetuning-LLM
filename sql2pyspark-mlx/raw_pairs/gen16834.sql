SELECT cost AS value FROM products WHERE cost IS NOT NULL UNION ALL SELECT amount AS value FROM transactions WHERE amount IS NOT NULL
