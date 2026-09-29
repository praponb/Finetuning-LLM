SELECT merchant, amount
FROM transactions
WHERE txn_time < '2024-05-01' AND txn_type = 'CREDIT'
ORDER BY merchant DESC;
