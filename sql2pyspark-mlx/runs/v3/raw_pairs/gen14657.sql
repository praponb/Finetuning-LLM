SELECT country FROM customers WHERE signup_date <= '2025-05-01' INTERSECT SELECT country FROM suppliers ORDER BY country DESC
