SELECT
    p.product_name,
    oi.unit_price,
    oi.quantity,
    oi.product_id
FROM products p
LEFT OUTER JOIN order_items oi ON p.id = oi.product_id
WHERE oi.product_id IN (7, 37) AND p.category != 'Home'
ORDER BY oi.unit_price DESC;
