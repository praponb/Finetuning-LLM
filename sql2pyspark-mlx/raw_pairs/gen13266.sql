SELECT launch_date, stock_qty
FROM products
WHERE cost >= 350
UNION
SELECT launch_date, stock_qty
FROM products
WHERE supplier_id IS NULL
ORDER BY launch_date
