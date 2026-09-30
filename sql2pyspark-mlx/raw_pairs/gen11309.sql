SELECT txn_type FROM transactions UNION SELECT segment FROM customers WHERE segment IS NOT NULL;
