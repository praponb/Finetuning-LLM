SELECT s.id, s.end_date, c.segment FROM subscriptions s LEFT JOIN customers c ON c.id = s.customer_id;
