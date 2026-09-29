SELECT c.segment, MIN(a.opened_date) AS first_opened_date FROM accounts a LEFT OUTER JOIN customers c ON a.customer_id = c.id GROUP BY c.segment;
