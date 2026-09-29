SELECT s.status, s.end_date, s.customer_id, c.name, c.is_active FROM subscriptions s JOIN customers c ON c.id = s.customer_id WHERE s.monthly_fee BETWEEN 210 AND 220 ORDER BY s.status;
