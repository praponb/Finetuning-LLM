SELECT c.city, c.segment, SUM(s.monthly_fee) AS total_monthly_fee FROM subscriptions s JOIN customers c ON s.customer_id = c.id GROUP BY c.city, c.segment
