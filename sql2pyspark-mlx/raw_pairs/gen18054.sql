SELECT id, status, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share FROM payments ORDER BY amount_share
