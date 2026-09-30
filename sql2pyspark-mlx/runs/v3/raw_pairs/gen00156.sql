SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE txn_type <> 'DEBIT')
PIVOT (AVG(amount) FOR merchant IN ('Starbucks', 'Shell'));
