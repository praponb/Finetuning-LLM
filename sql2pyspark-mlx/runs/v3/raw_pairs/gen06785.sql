SELECT * FROM (SELECT QUARTER(txn_time) AS txn_quarter, merchant, amount FROM transactions WHERE txn_time >= '2023-08-01')
PIVOT (AVG(amount) FOR merchant IN ('Grab', 'Starbucks', 'Shell'))
ORDER BY txn_quarter;
