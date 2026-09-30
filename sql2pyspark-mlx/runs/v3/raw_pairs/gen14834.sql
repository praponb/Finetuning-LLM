SELECT stock_qty, supplier_id
FROM products
WHERE product_name LIKE '%Chair%'
UNION ALL
SELECT stock_qty, supplier_id
FROM products
WHERE MONTH(launch_date) IN (5, 6, 7);
