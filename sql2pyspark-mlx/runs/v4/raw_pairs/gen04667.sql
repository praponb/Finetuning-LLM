SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE account_id IN (1, 15, 26, 32))
PIVOT (SUM(amount) FOR merchant IN ('Shell', 'Grab'));
