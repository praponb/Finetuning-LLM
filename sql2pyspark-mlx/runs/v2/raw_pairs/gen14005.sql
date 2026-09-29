SELECT id, country, signup_date FROM (SELECT *, RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS pos FROM customers) AS ranked WHERE pos = 1
