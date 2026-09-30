SELECT t.txn_type, a.customer_id, c.age, c.signup_date FROM transactions t LEFT OUTER JOIN accounts a ON a.id = t.account_id LEFT OUTER JOIN customers c ON a.customer_id = c.id;
