SELECT a.id, a.balance FROM accounts a WHERE a.account_type IN ('LOAN', 'CHECKING', 'CREDIT') AND a.customer_id NOT IN (SELECT id FROM customers WHERE segment IS NOT NULL) ORDER BY a.id DESC
