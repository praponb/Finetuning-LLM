SELECT email FROM employees WHERE email IS NOT NULL UNION SELECT plan AS email FROM subscriptions WHERE plan IS NOT NULL ORDER BY email DESC;
