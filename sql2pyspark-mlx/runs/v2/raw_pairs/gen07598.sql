SELECT *, CONCAT_WS('-', email, country) email_country_key, INITCAP(TRIM(name)) name_title FROM customers WHERE is_active = FALSE AND credit_limit < 5300;
