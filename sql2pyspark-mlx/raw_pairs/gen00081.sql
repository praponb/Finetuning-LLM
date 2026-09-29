SELECT unit_price
FROM order_items
WHERE (quantity > 8 AND order_id IN (17, 26, 32)) OR product_id IN (11, 13, 28, 33)
ORDER BY unit_price DESC
