SELECT c.city, s.status, s.customer_id, s.end_date FROM customers c LEFT JOIN subscriptions s ON s.customer_id = c.id WHERE s.monthly_fee < 130 ORDER BY c.city, s.customer_id DESC;
