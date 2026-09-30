SELECT id, category, price, DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_dense_rank FROM products;
