SELECT city, SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END) AS uk_count FROM suppliers WHERE city IS NOT NULL GROUP BY city
