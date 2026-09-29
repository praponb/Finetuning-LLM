SELECT amount, ROUND(amount, 0) AS amount_rounded
FROM transactions
WHERE account_id IN (29, 34, 38)
ORDER BY amount DESC;
