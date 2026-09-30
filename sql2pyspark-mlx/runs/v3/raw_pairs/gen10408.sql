SELECT
    p.category,
    p.cost,
    p.stock_qty,
    ss.product_id
FROM products p
LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id
ORDER BY stock_qty;
