SELECT city, COUNT(age) AS age_count FROM customers WHERE segment = 'Consumer' AND segment = 'SMB' GROUP BY city HAVING COUNT(age) > 5 ORDER BY city DESC;
