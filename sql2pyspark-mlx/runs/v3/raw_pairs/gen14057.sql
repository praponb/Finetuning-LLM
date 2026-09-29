SELECT *
FROM products
WHERE supplier_id IS NOT NULL AND stock_qty > 104
ORDER BY product_name ASC
