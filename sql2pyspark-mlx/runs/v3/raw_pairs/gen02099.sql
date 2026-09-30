SELECT id, price, DENSE_RANK() OVER (ORDER BY price DESC) AS dense_rnk
FROM products
ORDER BY price DESC
