SELECT id, launch_date
FROM products
WHERE supplier_id IN (3, 26, 28)
UNION
SELECT id, launch_date
FROM products
WHERE price BETWEEN 410 AND 860;
