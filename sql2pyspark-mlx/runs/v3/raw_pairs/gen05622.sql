SELECT sale_date, 'store_sale' AS source
FROM store_sales
UNION ALL
SELECT hire_date, 'employee' AS source
FROM employees
WHERE salary <= 79000;
