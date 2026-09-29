SELECT customer_id, account_type, MAX(balance) - MIN(balance) AS balance_range FROM accounts WHERE balance > 40000 GROUP BY customer_id, account_type HAVING MAX(balance) - MIN(balance) < 55;
