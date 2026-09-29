SELECT s.id, s.browser, s.country, c.country AS customer_country, c.age - c.credit_limit AS age_minus_credit_limit FROM sessions s INNER JOIN customers c ON s.customer_id = c.id;
