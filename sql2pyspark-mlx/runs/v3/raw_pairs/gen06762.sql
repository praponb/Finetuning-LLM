SELECT c.city, MIN(s.start_date) AS min_start_date, COUNT(s.monthly_fee) AS monthly_fee_count
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit < 7300
GROUP BY c.city;
