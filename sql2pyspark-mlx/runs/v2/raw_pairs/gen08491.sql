SELECT age FROM customers WHERE is_active = TRUE UNION ALL SELECT age FROM customers WHERE segment = 'Corporate' ORDER BY age DESC
