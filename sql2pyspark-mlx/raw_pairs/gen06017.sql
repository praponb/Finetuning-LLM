SELECT id, launch_date, stock_qty
FROM products
WHERE stock_qty BETWEEN 182 AND 322
EXCEPT
SELECT id, launch_date, stock_qty
FROM products
WHERE LENGTH(product_name) >= 12
