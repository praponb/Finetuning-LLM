SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount) AS dense_rnk
FROM orders;
