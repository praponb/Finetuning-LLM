SELECT txn_type, MIN(amount) AS min_amount, COUNT(*) AS cnt FROM transactions GROUP BY txn_type HAVING MIN(amount) > 3300
