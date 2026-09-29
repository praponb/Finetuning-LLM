SELECT id, segment, signup_date, email FROM (SELECT *, RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) rnk FROM customers WHERE is_active = TRUE) r WHERE rnk <= 3 ORDER BY email
