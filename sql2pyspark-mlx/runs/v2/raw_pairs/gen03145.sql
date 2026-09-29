SELECT
    brand,
    supplier_id,
    category,
    GREATEST(stock_qty, price) AS greatest_stock_qty_price
FROM products
WHERE LOWER(product_name) LIKE '%apt%' OR cost < 710
ORDER BY category ASC
