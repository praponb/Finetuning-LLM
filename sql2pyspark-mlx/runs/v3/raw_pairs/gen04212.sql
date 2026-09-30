SELECT segment, MIN(age) AS min_age, SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END) AS sg_count FROM customers WHERE age BETWEEN 41 AND 65 GROUP BY 1 ORDER BY sg_count;
