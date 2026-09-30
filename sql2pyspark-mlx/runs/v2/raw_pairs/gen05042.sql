SELECT id, segment FROM customers WHERE is_active INTERSECT SELECT id, segment FROM customers WHERE signup_date > '2023-08-08' ORDER BY segment DESC;
