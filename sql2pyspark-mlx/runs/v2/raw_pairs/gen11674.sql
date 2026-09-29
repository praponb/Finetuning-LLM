SELECT accounts.status, MAX(transactions.amount) AS max_amount, COUNT(*) AS cnt
FROM transactions
LEFT OUTER JOIN accounts ON transactions.account_id = accounts.id
JOIN customers ON accounts.customer_id = customers.id
GROUP BY accounts.status
ORDER BY cnt DESC, max_amount DESC
