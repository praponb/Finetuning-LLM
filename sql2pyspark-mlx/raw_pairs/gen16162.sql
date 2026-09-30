SELECT dim.city, a.account_type, COUNT(*) AS rows_total, ROUND(SUM(a.balance), 2) AS total_balance FROM accounts a LEFT JOIN customers dim ON a.customer_id = dim.id GROUP BY dim.city, a.account_type;
