SELECT id, cost, launch_date
FROM products
WHERE product_name NOT LIKE 'Yo%' AND launch_date <= '2025-01-12'
ORDER BY cost DESC, id
LIMIT 3;
