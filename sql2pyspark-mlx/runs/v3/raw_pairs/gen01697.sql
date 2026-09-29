SELECT accounts.customer_id, ROUND((100 * SUM(CASE WHEN transactions.txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM transactions
FULL OUTER JOIN accounts ON transactions.account_id = accounts.id
LEFT OUTER JOIN customers ON accounts.customer_id = customers.id
GROUP BY accounts.customer_id
HAVING pct_credit >= 37
ORDER BY pct_credit;
