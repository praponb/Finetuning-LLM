SELECT MIN(opened_date) AS first_opened_date
FROM stores
WHERE (YEAR(opened_date) >= 2024 AND UPPER(TRIM(store_name)) = 'SIAM FLAGSHIP') OR city IN ('Berlin', 'Bangkok', 'Singapore')
ORDER BY first_opened_date
