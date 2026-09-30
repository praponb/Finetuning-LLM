SELECT brand, stock_qty
FROM products
WHERE cost BETWEEN 360 AND 510
UNION ALL
SELECT brand, stock_qty
FROM products
WHERE LOWER(product_name) LIKE '%mar%';
