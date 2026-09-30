SELECT s.id, s.monthly_fee, c.city FROM subscriptions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE s.customer_id = 40 AND YEAR(s.start_date) = 2023 ORDER BY id DESC, city
