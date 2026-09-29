SELECT account_id, COUNT(*) AS row_count, MIN(amount) AS min_amount FROM transactions WHERE txn_type = 'FEE' GROUP BY account_id HAVING MIN(amount) > 2500 ORDER BY min_amount
