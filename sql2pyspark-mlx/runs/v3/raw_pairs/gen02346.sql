SELECT launch_date, brand
FROM products
WHERE category = 'Electronics'
UNION
SELECT launch_date, brand
FROM products
WHERE SIZE(tags) >= 2;
