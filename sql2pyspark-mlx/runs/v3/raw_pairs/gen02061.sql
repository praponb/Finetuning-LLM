SELECT customers.signup_date, customers.segment, t.row_count FROM customers LEFT JOIN (SELECT customer_id, COUNT(*) AS row_count FROM sessions GROUP BY customer_id) t ON customers.id = t.customer_id;
