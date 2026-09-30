SELECT a.id, a.status, a.balance, a.account_type, c.segment, c.credit_limit FROM accounts a RIGHT JOIN customers c ON a.customer_id = c.id WHERE c.age < 56;
