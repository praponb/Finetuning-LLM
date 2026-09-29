SELECT transactions.is_flagged, accounts.account_type, customers.age FROM transactions JOIN accounts ON transactions.account_id = accounts.id JOIN customers ON customers.id = accounts.customer_id;
