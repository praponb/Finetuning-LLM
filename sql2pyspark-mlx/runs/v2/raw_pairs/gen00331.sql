SELECT supplier_id
FROM products
WHERE supplier_id IS NULL
UNION ALL
SELECT supplier_id
FROM products
WHERE stock_qty < 121
ORDER BY supplier_id;
