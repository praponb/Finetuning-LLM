SELECT a.segment, b.level FROM (SELECT DISTINCT segment FROM customers) a CROSS JOIN (SELECT DISTINCT level FROM employees) b;
