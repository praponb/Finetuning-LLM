SELECT customer_id, COUNT(balance) AS balance_count, SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance
FROM accounts
GROUP BY customer_id;
