SELECT s.started_at, s.customer_id, c.signup_date FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE s.started_at < '2023-09-01' AND c.signup_date >= '2025-05-01';
