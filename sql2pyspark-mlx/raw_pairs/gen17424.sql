SELECT c.city, ROUND(AVG(a.balance), 2) AS avg_balance FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id WHERE a.account_type IN ('LOAN', 'CHECKING') GROUP BY c.city;
