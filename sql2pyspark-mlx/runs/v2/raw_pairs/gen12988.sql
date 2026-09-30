SELECT id, price, DENSE_RANK() OVER (ORDER BY price) AS price_dense_rank FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller');
