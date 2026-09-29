SELECT s.end_date, s.monthly_fee, s.status, c.age FROM subscriptions s LEFT OUTER JOIN customers c ON c.id = s.customer_id WHERE s.status = 'PAUSED' ORDER BY monthly_fee, age DESC;
