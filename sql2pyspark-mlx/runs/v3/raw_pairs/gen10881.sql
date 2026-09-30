SELECT a.id, a.account_type
FROM accounts a
WHERE a.balance < 63000 AND a.customer_id NOT IN (SELECT id FROM customers WHERE country IN ('DE', 'JP', 'SG'))
ORDER BY a.account_type
