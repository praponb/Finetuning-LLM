SELECT id, customer_id FROM subscriptions WHERE monthly_fee BETWEEN 200 AND 230 UNION SELECT id, customer_id FROM subscriptions WHERE monthly_fee >= 90 ORDER BY customer_id;
