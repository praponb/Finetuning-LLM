SELECT city FROM customers WHERE email LIKE '%@startup.io' UNION ALL SELECT city FROM suppliers WHERE city IN ('Bangkok', 'New York', 'London') ORDER BY city DESC
