SELECT id, launch_date, brand
FROM products
WHERE brand = 'Initech'
UNION ALL
SELECT id, launch_date, brand
FROM products
WHERE cost > 530
ORDER BY brand DESC;
