SELECT brand, tags, CEIL(stock_qty) stock_qty_whole
FROM products
WHERE supplier_id IN (8, 18, 20) AND brand IN ('Initech', 'Globex', 'Stark')
ORDER BY brand ASC
