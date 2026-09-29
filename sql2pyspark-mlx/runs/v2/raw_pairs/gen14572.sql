SELECT accounts.id, accounts.opened_date, accounts.balance
FROM accounts
WHERE accounts.balance >= 41000 AND accounts.customer_id NOT IN (SELECT id FROM customers WHERE is_active = FALSE);
