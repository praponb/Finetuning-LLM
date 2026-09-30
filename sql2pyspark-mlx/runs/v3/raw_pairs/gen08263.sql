SELECT unit_price, product_id
FROM order_items
WHERE order_id IN (7, 11) AND unit_price <= 270
ORDER BY unit_price DESC;
