SELECT
    p.id,
    p.category,
    p.stock_qty,
    p.supplier_id
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
ORDER BY category
