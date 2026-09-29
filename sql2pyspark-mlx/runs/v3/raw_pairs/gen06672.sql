SELECT id, brand, category
FROM products
WHERE launch_date <= '2024-01-01'
UNION ALL
SELECT id, brand, category
FROM products
WHERE ARRAY_CONTAINS(tags, 'premium');
