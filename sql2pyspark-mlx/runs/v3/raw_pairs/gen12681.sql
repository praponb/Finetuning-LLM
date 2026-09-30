SELECT status, 'order' AS source FROM orders UNION SELECT status, 'account' AS source FROM accounts WHERE opened_date <= '2023-03-27';
