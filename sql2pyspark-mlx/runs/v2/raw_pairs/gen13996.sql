SELECT id, amount, account_id
FROM transactions
WHERE amount <= (SELECT MIN(amount) FROM transactions WHERE NOT is_flagged)
ORDER BY amount DESC;
