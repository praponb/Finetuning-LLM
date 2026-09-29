SELECT supplier_name
FROM suppliers
WHERE supplier_name IS NOT NULL
EXCEPT
SELECT job_title AS supplier_name
FROM employees
WHERE job_title IS NOT NULL;
