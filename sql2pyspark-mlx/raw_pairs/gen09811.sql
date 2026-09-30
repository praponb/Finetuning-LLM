SELECT customers.email, customers.credit_limit, accounts.customer_id, accounts.account_type, accounts.opened_date FROM customers JOIN accounts ON customers.id = accounts.customer_id;
