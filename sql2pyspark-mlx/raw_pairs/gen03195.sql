SELECT 'store' origin, region
FROM stores
WHERE opened_date <= '2024-10-01'
UNION ALL
SELECT 'employee' origin, level region
FROM employees
WHERE is_remote = TRUE;
