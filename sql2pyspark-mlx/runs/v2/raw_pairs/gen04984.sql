SELECT a.id, a.account_type, a.balance FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE name NOT LIKE '%Ivy%') ORDER BY a.id DESC;
