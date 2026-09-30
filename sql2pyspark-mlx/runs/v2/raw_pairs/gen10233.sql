SELECT tags, stock_qty, category
FROM products
WHERE cost >= 220 AND ARRAY_CONTAINS(tags, 'bestseller')
ORDER BY category, stock_qty
