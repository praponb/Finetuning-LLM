SELECT *, ROUND(amount * 100, 2) AS amount_adjusted
FROM transactions
WHERE amount BETWEEN 1300 AND 1700 AND account_id IN (18, 25, 32, 33);
