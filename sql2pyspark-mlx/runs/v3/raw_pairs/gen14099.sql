SELECT
    p.category,
    p.stock_qty,
    p.brand,
    oi.unit_price,
    oi.quantity,
    oi.order_id
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE p.category <> 'Grocery' AND ARRAY_CONTAINS(p.tags, 'new')
