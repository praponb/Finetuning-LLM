SELECT COUNT(*) cnt, COUNT(DISTINCT stock_qty) unique_suppliers
FROM products
WHERE product_name IS NOT NULL
ORDER BY cnt DESC;
