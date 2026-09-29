SELECT x.city, y.level FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT level FROM employees) y
