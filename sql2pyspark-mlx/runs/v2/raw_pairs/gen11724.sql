SELECT a.id, a.customer_id, t.avg_amount FROM accounts a LEFT JOIN (SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount FROM transactions GROUP BY account_id) t ON a.id = t.account_id;
