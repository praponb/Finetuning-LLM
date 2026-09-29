SELECT s.id, s.status, c.credit_limit FROM subscriptions s LEFT OUTER JOIN customers c ON s.customer_id = c.id WHERE c.signup_date > '2023-05-20' AND c.city = 'Chiang Mai';
