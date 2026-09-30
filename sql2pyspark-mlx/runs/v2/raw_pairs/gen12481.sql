SELECT id, unit_price, ROUND(quantity, 2) quantity_rounded
FROM order_items
WHERE product_id = 16
