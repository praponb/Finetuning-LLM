SELECT id, amount, RANK() OVER (ORDER BY amount) AS amount_rank
FROM payments
WHERE amount < 1590;
