SELECT a.id, a.balance, c.age FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE a.account_type != 'CREDIT' ORDER BY age DESC, balance ASC;
