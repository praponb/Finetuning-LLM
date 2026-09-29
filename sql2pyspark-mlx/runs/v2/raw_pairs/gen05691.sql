SELECT is_active, SUBSTRING(segment, 1, 2) AS segment_prefix FROM customers WHERE (is_active = FALSE AND credit_limit BETWEEN 5700 AND 7400) OR email LIKE '%@startup.io';
