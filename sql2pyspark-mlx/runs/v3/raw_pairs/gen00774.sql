SELECT accounts.id, accounts.account_type, customers.signup_date FROM accounts LEFT JOIN customers ON accounts.customer_id = customers.id
