SELECT s.end_date, s.customer_id, s.plan, c.email, c.signup_date, c.credit_limit FROM subscriptions s FULL OUTER JOIN customers c ON s.customer_id = c.id WHERE LENGTH(c.email) < 10 ORDER BY end_date;
