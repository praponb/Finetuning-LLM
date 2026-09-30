SELECT id, country, segment, signup_date, NTILE(4) OVER (PARTITION BY country, segment ORDER BY signup_date, id) AS quartile FROM customers ORDER BY id DESC;
