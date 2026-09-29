SELECT supplier_id, category
FROM products
WHERE UPPER(product_name) LIKE '%FFI%'
UNION ALL
SELECT supplier_id, category
FROM products
WHERE launch_date BETWEEN '2023-04-01' AND '2024-05-01'
