SELECT id, city, CAST(is_active AS INT) AS is_active_flag FROM customers WHERE credit_limit > 11200 AND email LIKE '%@startup.io' AND credit_limit BETWEEN 6500 AND 14200 ORDER BY id
