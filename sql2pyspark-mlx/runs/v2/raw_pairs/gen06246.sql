SELECT id, brand, stock_qty
FROM products
WHERE supplier_id IN (16, 36)
UNION ALL
SELECT id, brand, stock_qty
FROM products
WHERE brand NOT IN ('Umbrella', 'Stark', 'Initech')
ORDER BY stock_qty DESC;
