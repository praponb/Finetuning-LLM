SELECT cost, 'product' AS origin FROM products WHERE cost IS NOT NULL UNION ALL SELECT amount, 'order' AS origin FROM orders WHERE amount IS NOT NULL ORDER BY cost;
