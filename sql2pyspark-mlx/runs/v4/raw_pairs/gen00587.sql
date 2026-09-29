SELECT id, amount, account_id
FROM transactions
WHERE amount > (SELECT MAX(amount) FROM transactions WHERE txn_time >= '2024-09-18');
