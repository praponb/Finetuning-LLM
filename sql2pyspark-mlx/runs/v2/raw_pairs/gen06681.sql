SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE is_flagged) PIVOT (SUM(amount) FOR merchant IN ('Starbucks', 'Amazon', 'Lazada'))
