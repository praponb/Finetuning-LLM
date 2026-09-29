SELECT txn_time, amount, txn_type
FROM transactions
WHERE (amount BETWEEN 1300 AND 2000 AND amount <= 3800) OR is_flagged = FALSE
