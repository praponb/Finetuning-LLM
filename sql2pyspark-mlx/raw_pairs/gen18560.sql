SELECT
    a.id,
    a.balance,
    a.status,
    a.account_type
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit > 14700)
ORDER BY account_type ASC, id ASC
LIMIT 20;
