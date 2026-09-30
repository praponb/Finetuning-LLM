SELECT brand, supplier_id, ROUND(stock_qty, 2) AS stock_qty_rounded
FROM products
WHERE (supplier_id IS NOT NULL AND stock_qty != 200) OR ARRAY_CONTAINS(tags, 'clearance');
