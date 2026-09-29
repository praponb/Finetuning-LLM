SELECT c.city, SUM(CASE WHEN a.account_type = 'CHECKING' THEN a.balance ELSE 0 END) checking_balance
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE a.account_type IN ('CHECKING', 'LOAN')
GROUP BY c.city
ORDER BY checking_balance DESC, c.city DESC
