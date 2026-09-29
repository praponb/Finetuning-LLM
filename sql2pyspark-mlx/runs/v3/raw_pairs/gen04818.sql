SELECT *, CAST(is_active AS INT) AS is_active_flag FROM customers WHERE name IS NOT NULL AND (signup_date BETWEEN '2023-07-12' AND '2023-10-01' OR country != 'US');
