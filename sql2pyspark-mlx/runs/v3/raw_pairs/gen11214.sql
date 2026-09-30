SELECT ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
WHERE account_id IN (9, 13, 21, 34) AND NOT is_flagged
