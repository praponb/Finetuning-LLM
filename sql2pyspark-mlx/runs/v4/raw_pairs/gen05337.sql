SELECT p.supplier_id, ss.units
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
ORDER BY supplier_id DESC
