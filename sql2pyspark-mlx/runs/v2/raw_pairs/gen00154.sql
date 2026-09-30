SELECT id, city, signup_date, country FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY signup_date DESC) AS rnk FROM customers WHERE is_active = TRUE) AS r WHERE rnk <= 3
