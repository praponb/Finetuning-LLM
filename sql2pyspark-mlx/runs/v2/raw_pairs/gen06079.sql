SELECT accounts.id, accounts.opened_date FROM accounts WHERE accounts.status <> 'CLOSED' AND accounts.customer_id IN (SELECT id FROM customers WHERE signup_date BETWEEN '2024-04-01' AND '2024-09-23')
