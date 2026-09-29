SELECT DISTINCT city, country FROM customers WHERE city NOT IN ('New York', 'Berlin', 'Bangkok') ORDER BY city DESC;
