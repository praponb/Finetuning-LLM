SELECT
    s.id,
    s.rating,
    s.supplier_name,
    t.max_stock_qty,
    t.price_count
FROM suppliers s
LEFT JOIN (SELECT supplier_id, MAX(stock_qty) AS max_stock_qty, COUNT(price) AS price_count
    FROM products
    WHERE product_name LIKE '%Nois%'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.max_stock_qty >= 370
ORDER BY supplier_name;
