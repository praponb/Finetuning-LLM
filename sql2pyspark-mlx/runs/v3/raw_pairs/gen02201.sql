SELECT country FROM customers WHERE city IN ('Chiang Mai', 'Bangkok', 'Tokyo') UNION SELECT country FROM suppliers;
