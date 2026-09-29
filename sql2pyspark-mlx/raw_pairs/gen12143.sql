SELECT c.city, COUNT(a.balance) AS balance_count, ROUND(STDDEV(a.balance), 2) AS stddev_balance
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE a.account_type IN ('CHECKING', 'CREDIT')
GROUP BY c.city
ORDER BY balance_count DESC
