SELECT s.started_at, c.country, c.age * c.credit_limit AS age_times_credit_limit FROM sessions s RIGHT OUTER JOIN customers c ON s.customer_id = c.id WHERE c.age >= 56 ORDER BY country;
