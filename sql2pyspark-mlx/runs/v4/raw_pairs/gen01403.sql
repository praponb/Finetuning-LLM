SELECT customers.country, SUM(CASE WHEN transactions.txn_type = 'FEE' THEN transactions.amount ELSE 0 END) AS fee_amount, SUM(CASE WHEN transactions.txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count
FROM transactions
INNER JOIN accounts ON accounts.id = transactions.account_id
INNER JOIN customers ON customers.id = accounts.customer_id
GROUP BY customers.country
ORDER BY customers.country DESC, fee_amount ASC;
