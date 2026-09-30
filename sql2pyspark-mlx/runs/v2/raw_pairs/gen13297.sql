SELECT id, amount, DENSE_RANK() OVER (ORDER BY amount DESC) AS amount_dense_rank
FROM payments
WHERE amount BETWEEN 690 AND 1160
ORDER BY amount, amount_dense_rank DESC
