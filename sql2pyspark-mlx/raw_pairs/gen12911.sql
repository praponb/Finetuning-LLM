SELECT id, email FROM customers WHERE is_active = TRUE INTERSECT SELECT id, email FROM customers WHERE credit_limit >= 6600 ORDER BY email DESC
