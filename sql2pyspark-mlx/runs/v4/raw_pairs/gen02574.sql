SELECT
    p.product_name,
    p.brand,
    p.cost,
    oi.quantity
FROM products p
LEFT OUTER JOIN order_items oi ON p.id = oi.product_id
WHERE oi.unit_price >= 230
