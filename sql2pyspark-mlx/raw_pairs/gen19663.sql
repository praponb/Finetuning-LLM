SELECT launch_date
FROM products
WHERE supplier_id = 33
UNION
SELECT launch_date
FROM products
WHERE price < 690
ORDER BY launch_date;
