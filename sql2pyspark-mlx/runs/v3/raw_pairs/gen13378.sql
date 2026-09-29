SELECT COUNT(*) product_count, COUNT(DISTINCT price) unique_suppliers
FROM products
WHERE supplier_id IS NULL AND launch_date BETWEEN '2023-08-26' AND '2024-02-19'
ORDER BY product_count DESC
