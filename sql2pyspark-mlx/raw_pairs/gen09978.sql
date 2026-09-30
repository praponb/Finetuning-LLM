SELECT quantity, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
FROM order_items
WHERE product_id IN (19, 29)
ORDER BY greatest_unit_price_quantity DESC
