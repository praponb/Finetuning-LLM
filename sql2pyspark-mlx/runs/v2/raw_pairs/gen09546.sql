SELECT c.city, COUNT(*) AS num_subscriptions FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id WHERE s.monthly_fee >= 70 GROUP BY c.city HAVING num_subscriptions >= 5;
