SELECT * FROM (SELECT txn_type, account_id, merchant, amount FROM transactions) src PIVOT (MAX(amount) AS value_sum FOR merchant IN ('Lazada', 'Starbucks'))
