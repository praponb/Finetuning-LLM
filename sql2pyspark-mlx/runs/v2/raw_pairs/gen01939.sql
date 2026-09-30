SELECT
    p.brand,
    p.price,
    oi.product_id,
    LEAST(oi.unit_price, oi.quantity) least_unit_price_quantity
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
WHERE oi.unit_price BETWEEN 230 AND 390 OR oi.quantity <= 4
