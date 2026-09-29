SELECT
    ss.id,
    ss.revenue,
    ss.product_id,
    p.stock_qty,
    p.price,
    s.rating
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.rating < 3.9;
