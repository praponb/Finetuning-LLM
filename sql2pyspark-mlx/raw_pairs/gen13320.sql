SELECT amount AS value FROM payments WHERE paid_at >= '2023-09-27' UNION ALL SELECT cost AS value FROM products;
