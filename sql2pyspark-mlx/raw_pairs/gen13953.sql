SELECT SUM(age) AS total_age FROM customers WHERE age BETWEEN 37 AND 58 AND (city = 'Bangkok' OR city IN ('London', 'Bangkok'));
