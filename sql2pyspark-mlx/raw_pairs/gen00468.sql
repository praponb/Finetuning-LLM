SELECT segment FROM customers WHERE segment IS NOT NULL INTERSECT SELECT job_title FROM employees WHERE job_title IS NOT NULL ORDER BY segment
