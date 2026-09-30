SELECT country FROM customers WHERE age < 41 EXCEPT SELECT country FROM sessions WHERE customer_id IN (2, 3);
