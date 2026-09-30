SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE merchant IN ('Starbucks', '7-Eleven'))
PIVOT (COUNT(amount) FOR merchant IN ('7-Eleven', 'Lazada'));
