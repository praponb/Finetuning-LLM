SELECT x.segment, y.level FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT level FROM employees) y
