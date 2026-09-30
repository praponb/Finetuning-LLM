SELECT status FROM shipments EXCEPT SELECT country FROM customers WHERE MONTH(signup_date) IN (1, 5, 11) ORDER BY status
