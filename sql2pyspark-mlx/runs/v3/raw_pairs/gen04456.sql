SELECT
    p.id,
    p.category,
    COUNT(oi.id) AS order_item_count,
    COUNT(oi.unit_price) AS unit_price_count,
    MAX(oi.unit_price) AS max_unit_price
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE p.brand NOT IN ('Acme', 'Initech', 'Umbrella')
GROUP BY p.id, p.category
ORDER BY id DESC, order_item_count
