SELECT 'employee' AS source, hire_date
FROM employees
UNION ALL
SELECT 'store_sale' AS source, sale_date
FROM store_sales
WHERE revenue >= 3500
ORDER BY hire_date
