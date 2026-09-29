SELECT segment, age, is_active, CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk FROM customers WHERE age < 58 ORDER BY segment;
