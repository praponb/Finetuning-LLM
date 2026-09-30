SELECT s.id, s.end_date, s.plan, s.status, c.segment FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY s.plan ASC, s.id ASC LIMIT 10;
