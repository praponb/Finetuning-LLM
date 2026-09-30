SELECT a.status, a.customer_id, t.total_amount FROM accounts a INNER JOIN (SELECT account_id, SUM(amount) AS total_amount FROM transactions GROUP BY account_id) t ON a.id = t.account_id;
