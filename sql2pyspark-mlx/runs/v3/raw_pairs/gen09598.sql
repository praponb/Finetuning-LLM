SELECT credit_limit, signup_date, country, INITCAP(TRIM(name)) AS name_title FROM customers WHERE is_active = TRUE
