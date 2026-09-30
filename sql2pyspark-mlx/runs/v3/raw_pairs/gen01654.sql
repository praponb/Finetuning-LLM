SELECT *, CAST(is_active AS INT) AS is_active_flag FROM customers WHERE signup_date < '2024-04-01' AND is_active = TRUE;
