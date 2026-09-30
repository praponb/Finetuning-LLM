SELECT city FROM customers WHERE credit_limit > 8300 EXCEPT SELECT city FROM stores WHERE region NOT IN ('South', 'East');
