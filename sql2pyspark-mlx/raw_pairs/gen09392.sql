SELECT a.id, a.opened_date FROM accounts a WHERE a.account_type IN ('SAVINGS', 'CHECKING') AND a.customer_id NOT IN (SELECT id FROM customers WHERE age IS NULL)
