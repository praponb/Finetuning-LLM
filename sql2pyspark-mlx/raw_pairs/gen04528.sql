SELECT id, method, amount, DENSE_RANK() OVER (PARTITION BY method ORDER BY amount) AS dense_rnk, AVG(amount) OVER (PARTITION BY method) AS method_avg_amount FROM payments;
