SELECT p.brand, oi.unit_price
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
WHERE oi.product_id = 15
ORDER BY unit_price, brand DESC
