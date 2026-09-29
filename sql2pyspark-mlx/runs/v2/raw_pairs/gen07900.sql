SELECT accounts.balance, accounts.status, accounts.opened_date, customers.signup_date, customers.email FROM accounts INNER JOIN customers ON accounts.customer_id = customers.id;
