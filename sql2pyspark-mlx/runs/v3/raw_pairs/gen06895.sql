SELECT ss.units, p.brand
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty < 150 OR ss.units <= 22
ORDER BY ss.units DESC
