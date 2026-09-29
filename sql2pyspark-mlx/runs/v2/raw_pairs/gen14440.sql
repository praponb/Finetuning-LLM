SELECT a.opened_date, a.customer_id, c.credit_limit FROM accounts a LEFT OUTER JOIN customers c ON a.customer_id = c.id ORDER BY a.opened_date;
